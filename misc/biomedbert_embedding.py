import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel


class SentenceEmbedder:
    def __init__(self, model_name="microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext", max_seq_length=512):
        """
        Initializes the SentenceEmbedder with a pre-trained model.

        Args:
            model_name (str): The name of the pre-trained model to use from Hugging Face.
            max_seq_length (int): The maximum sequence length the model can handle.
        """
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.model.eval() # Set the model to evaluation mode
        self.max_seq_length = max_seq_length

    def encode(self, sentences):
        """
        Encodes a list of sentences (or a single sentence) into embeddings using the [CLS] token representation.
        Handles long texts by splitting into chunks and averaging embeddings.

        Args:
            sentences (list or str): A list of strings or a single string (sentence/text).

        Returns:
            torch.Tensor: A tensor containing the [CLS] token embeddings for each sentence/text.
                          Shape will be (number of inputs, embedding dimension).
        """
        if not isinstance(sentences, list):
            sentences = [sentences] # Handle single sentence input

        all_embeddings = []
        for sentence in sentences:
            # Tokenize the entire sentence first
            tokens = self.tokenizer.tokenize(sentence)
            token_length = len(tokens)

            if token_length <= self.max_seq_length - 2: # Account for [CLS] and [SEP]
                # Encode directly if within max sequence length
                inputs = self.tokenizer(sentence, padding=True, truncation=True, return_tensors="pt", max_length=self.max_seq_length)
                with torch.no_grad():
                    outputs = self.model(**inputs, output_hidden_states=True)
                # The last hidden state is in outputs.hidden_states[-1]
                # The embedding for the [CLS] token is the first vector in the sequence
                cls_embedding = outputs.hidden_states[-1][:, 0, :].squeeze(0) # Remove batch dim for single sentence
                all_embeddings.append(cls_embedding)
            else:
                # Handle long text by splitting into overlapping chunks
                chunk_size = self.max_seq_length - 2 # Account for [CLS] and [SEP]
                overlap = chunk_size // 2 # Example overlap

                chunk_embeddings = []
                # Iterate through tokens with overlap
                for i in range(0, token_length, chunk_size - overlap):
                    chunk_tokens = tokens[i : i + chunk_size]

                    # Convert chunk tokens back to a string and tokenize with padding/truncation
                    # Ensure each chunk input is exactly max_seq_length after adding special tokens
                    chunk_text = self.tokenizer.convert_tokens_to_string(chunk_tokens)
                    inputs = self.tokenizer(chunk_text, padding='max_length', truncation=True, return_tensors="pt", max_length=self.max_seq_length)

                    with torch.no_grad():
                        outputs = self.model(**inputs, output_hidden_states=True)

                    # Get the [CLS] token embedding for the chunk
                    cls_embedding = outputs.hidden_states[-1][:, 0, :].squeeze(0) # Remove batch dim for single chunk
                    chunk_embeddings.append(cls_embedding)

                # Average the chunk embeddings
                averaged_embedding = torch.mean(torch.stack(chunk_embeddings), dim=0)
                all_embeddings.append(averaged_embedding)

        return torch.stack(all_embeddings) # Stack all sentence/text embeddings


    def get_similarity(self, sentence1, sentence2):
        """
        Calculates the cosine similarity between the embeddings of two sentences.

        Args:
            sentence1 (str): The first sentence.
            sentence2 (str): The second sentence.

        Returns:
            float: The cosine similarity between the embeddings of the two sentences.
                   Returns None if encoding fails for either sentence.
        """
        try:
            # Encode both sentences (will handle long texts internally)
            embeddings = self.encode([sentence1, sentence2])

            # Get the embeddings for each sentence
            embedding1 = embeddings[0].unsqueeze(0) # Add a batch dimension
            embedding2 = embeddings[1].unsqueeze(0) # Add a batch dimension

            # Calculate cosine similarity
            similarity = F.cosine_similarity(embedding1, embedding2)

            return similarity.item() # Return the scalar value
        except Exception as e:
            print(f"Error calculating similarity: {e}")
            return None

# Example usage (requires the 'embedder' object with a tokenizer from previous cells):
embedder = SentenceEmbedder()
sentences = [
    "The patient presented with a severe pain in the chest.",
    "The study investigated the effects of the new drug.",
    "Biomedical natural language processing is a challenging task."
]
embeddings = embedder.encode(sentences)
print("Embeddings shape:", embeddings.shape)
print("Embeddings:", embeddings)

# Example similarity calculation:
sentence_a = "The patient has a high fever."
sentence_b = "The person's temperature is elevated."
similarity_score = embedder.get_similarity(sentence_a, sentence_b)
print(f"Similarity between '{sentence_a}' and '{sentence_b}': {similarity_score}")