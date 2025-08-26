import boto3
import json
import csv
import os
import time
from pathlib import Path
from docx import Document
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from botocore.exceptions import ClientError, BotoCoreError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_MAX_TOKENS = 4000
DEFAULT_EXECUTIONS_PER_DOC = 3
API_CALL_DELAY = 1.0  # seconds between API calls
MAX_DOCUMENT_SIZE = 1_000_000  # characters

class BedrockClaudeProcessor:
    def __init__(self, aws_access_key_id: str = None, aws_secret_access_key: str = None, region_name: str = 'us-east-1'):
        """
        Initialize the Bedrock Claude processor
        
        Args:
            aws_access_key_id: AWS Access Key ID (if None, will use environment variables or IAM role)
            aws_secret_access_key: AWS Secret Access Key (if None, will use environment variables or IAM role)
            region_name: AWS region name
        """
        if aws_access_key_id and aws_secret_access_key:
            self.bedrock_client = boto3.client(
                'bedrock-runtime',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
        else:
            # Use default credential chain (environment variables, IAM role, etc.)
            self.bedrock_client = boto3.client('bedrock-runtime', region_name=region_name)
        
        # Verify this model ID with current AWS Bedrock documentation
        self.model_id = "us.anthropic.claude-3-5-sonnet-20241022-v2:0"
    
    def read_prompt_template(self, prompt_file_path: str) -> str:
        """
        Read the prompt template from file
        
        Args:
            prompt_file_path: Path to the prompt template file
            
        Returns:
            Content of the prompt template
        """
        try:
            with open(prompt_file_path, 'r', encoding='utf-8') as file:
                return file.read()
        except FileNotFoundError:
            logger.error(f"Prompt file not found: {prompt_file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading prompt file: {e}")
            raise
    
    def read_docx_file(self, file_path: str) -> str:
        """
        Read content from a .docx file with size validation
        
        Args:
            file_path: Path to the .docx file
            
        Returns:
            Text content of the document
            
        Raises:
            ValueError: If document is too large
        """
        try:
            doc = Document(file_path)
            full_text = []
            for paragraph in doc.paragraphs:
                full_text.append(paragraph.text)
            
            content = '\n'.join(full_text)
            
            # Validate document size
            if len(content) > MAX_DOCUMENT_SIZE:
                raise ValueError(f"Document too large: {len(content)} characters (max: {MAX_DOCUMENT_SIZE})")
            
            return content
            
        except Exception as e:
            logger.error(f"Error reading docx file {file_path}: {e}")
            raise
    
    def get_docx_files(self, folder_path: str) -> List[str]:
        """
        Get all .docx files from the specified folder
        
        Args:
            folder_path: Path to the folder containing .docx files
            
        Returns:
            List of .docx file paths
        """
        folder = Path(folder_path)
        if not folder.exists():
            raise FileNotFoundError(f"Folder not found: {folder_path}")
        
        docx_files = list(folder.glob("*.docx"))
        # Filter out temporary files (starting with ~$)
        docx_files = [f for f in docx_files if not f.name.startswith("~$")]
        
        logger.info(f"Found {len(docx_files)} .docx files in {folder_path}")
        return [str(f) for f in docx_files]
    
    def call_claude(self, prompt: str, max_tokens: int = DEFAULT_MAX_TOKENS) -> Dict[str, Any]:
        """
        Call Claude 3.5 Sonnet via Amazon Bedrock with retry logic
        
        Args:
            prompt: The prompt to send to Claude
            max_tokens: Maximum number of tokens to generate
            
        Returns:
            Dictionary containing the response and metadata
            
        Raises:
            ClientError: If AWS API call fails
        """
        max_retries = 3
        base_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                body = json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": max_tokens,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "temperature": 0.7
                })
                
                response = self.bedrock_client.invoke_model(
                    modelId=self.model_id,
                    body=body,
                    contentType='application/json'
                )
                
                response_body = json.loads(response['body'].read())
                
                # Add delay to prevent rate limiting
                time.sleep(API_CALL_DELAY)
                
                return {
                    'output': response_body['content'][0]['text'],
                    'token_count': response_body['usage']['output_tokens'],
                    'input_tokens': response_body['usage']['input_tokens']
                }
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['ThrottlingException', 'ServiceQuotaExceededException'] and attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Rate limited, retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"AWS API error: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error calling Claude: {e}")
                raise
    
    def process_documents(self, 
                         prompt_file_path: str, 
                         documents_folder_path: str, 
                         output_csv_path: str,
                         max_tokens: int = DEFAULT_MAX_TOKENS,
                         executions_per_document: int = DEFAULT_EXECUTIONS_PER_DOC):
        """
        Process all documents in the folder and save results to CSV
        
        Args:
            prompt_file_path: Path to the prompt template file
            documents_folder_path: Path to folder containing .docx files
            output_csv_path: Path for the output CSV file
            max_tokens: Maximum tokens per Claude call
            executions_per_document: Number of times to execute each document (default: 3)
        """
        # Read prompt template
        prompt_template = self.read_prompt_template(prompt_file_path)
        
        if "[[document]]" not in prompt_template:
            raise ValueError("Prompt template must contain the [[document]] placeholder")
        
        # Get all docx files
        docx_files = self.get_docx_files(documents_folder_path)
        
        if not docx_files:
            logger.warning("No .docx files found in the specified folder")
            return
        
        # Prepare CSV output
        results = []
        
        # Process each document
        for doc_file_path in docx_files:
            logger.info(f"Processing document: {doc_file_path}")
            
            # Read document content
            try:
                document_content = self.read_docx_file(doc_file_path)
                
                # Replace placeholder with document content
                full_prompt = prompt_template.replace("[[document]]", document_content)
                
                # Execute the prompt multiple times for this document
                for execution_num in range(executions_per_document):
                    logger.info(f"Execution {execution_num + 1}/{executions_per_document} for {os.path.basename(doc_file_path)}")
                    
                    try:
                        response = self.call_claude(full_prompt, max_tokens)
                        
                        results.append({
                            'input': os.path.basename(doc_file_path),
                            'output': response['output'],
                            'token_count': response['token_count']
                        })
                        
                        logger.info(f"Successfully processed execution {execution_num + 1} for {os.path.basename(doc_file_path)} (tokens: {response['token_count']})")
                        
                    except Exception as e:
                        logger.error(f"Error processing execution {execution_num + 1} for {doc_file_path}: {e}")
                        # Add error row with proper error handling
                        results.append({
                            'input': os.path.basename(doc_file_path),
                            'output': f"ERROR: {type(e).__name__}: {str(e)}",
                            'token_count': -1  # Use -1 to indicate error
                        })
                        
            except Exception as e:
                logger.error(f"Error reading document {doc_file_path}: {e}")
                continue
        
        # Save results to CSV
        self.save_to_csv(results, output_csv_path)
        logger.info(f"Results saved to {output_csv_path}")
    
    def save_to_csv(self, results: List[Dict], output_path: str):
        """
        Save results to CSV file with the exact specified headers
        
        Args:
            results: List of result dictionaries
            output_path: Path for the output CSV file
        """
        # Use exact headers as specified in requirements
        fieldnames = ['input', 'output', 'token_count']
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for result in results:
                writer.writerow(result)

def main():
    """
    Main function to run the document processing
    """
    # Configuration
    PROMPT_FILE_PATH = "./prompts/prompt_caso1.txt"  # Path to your prompt template file, alter it accordingly
    DOCUMENTS_FOLDER_PATH = "./anonimized_decisions/"      # Path to folder containing .docx files
    OUTPUT_CSV_PATH = "caso_1_results.csv"     # Output CSV file path
    
    # AWS credentials (optional - can be set as environment variables or use IAM role)
    # If using environment variables, set them like:
    # export AWS_ACCESS_KEY_ID=your_access_key_id
    # export AWS_SECRET_ACCESS_KEY=your_secret_access_key
    AWS_ACCESS_KEY_ID = None  # Set to your access key ID or leave as None to use environment variables
    AWS_SECRET_ACCESS_KEY = None  # Set to your secret access key or leave as None to use environment variables
    AWS_REGION = 'us-east-1'  # Change to your preferred region
    
    try:
        # Initialize processor
        processor = BedrockClaudeProcessor(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        # Process documents
        processor.process_documents(
            prompt_file_path=PROMPT_FILE_PATH,
            documents_folder_path=DOCUMENTS_FOLDER_PATH,
            output_csv_path=OUTPUT_CSV_PATH,
            max_tokens=DEFAULT_MAX_TOKENS,
            executions_per_document=DEFAULT_EXECUTIONS_PER_DOC
        )
        
        print(f"Processing completed successfully! Results saved to {OUTPUT_CSV_PATH}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()