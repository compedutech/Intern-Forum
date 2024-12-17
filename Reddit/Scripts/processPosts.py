import os
import sys
import csv
import string
from datetime import datetime
from transformers import pipeline
import logging.handlers
import pandas as pd
import re 
#import nltk
#from nltk.corpus import stopwords

    

input_file = r"/Reddit2023"
output_file = r"/NonComp2023"
output_file_c = r"/Comp2023"

# sets up logging to the console as well as a file
log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
log_str_handler = logging.StreamHandler()
log_str_handler.setFormatter(log_formatter)
log.addHandler(log_str_handler)
if not os.path.exists("logs"):
	os.makedirs("logs")
log_file_handler = logging.handlers.RotatingFileHandler(os.path.join("logs", "bot.log"), maxBytes=1024*1024*16, backupCount=5)
log_file_handler.setFormatter(log_formatter)
log.addHandler(log_file_handler)
handle_stat = open(r"/Stat/stat.csv", 'w', encoding='UTF-8', newline='')
writer_stat = csv.writer(handle_stat)
#cachedStopWords = stopwords.words("english")
#cachedStopWords = nltk.download('stopwords')
#print(cachedStopWords)
#print(type(cachedStopWords))


def process_file(pipe,input_file, output_file,output_file_c):
	output_format = "csv"
	output_path = f"{output_file}.{output_format}" 
	output_path_c = f"{output_file_c}.{output_format}"
	log.info(f"Input: {input_file} ")
	handle = open(output_path, 'w', encoding='UTF-8', newline='')
	writer = csv.writer(handle)
    
	handle_c = open(output_path_c, 'w', encoding='UTF-8', newline='')
	writer_c = csv.writer(handle_c)
    
	input_handle = open(input_file, mode='r',encoding='UTF-8',newline='',errors='ignore')

	file_size = os.stat(input_file).st_size
	total_post =0
	comp_post=0
	non_comp_post=0
	
	for line in csv.reader(input_handle):
		try:
			text = line[7]
			#text=preprocess_text(line[7])
			#log.info(f"input text : {text} ")
			if text== "":
				writer.writerow(line)
			else:
				total_post +=1
				class_ = classify_comment(pipe,text)
				#log.info(f"predicted class : {class_} ")
				if class_ =="computing":
					writer_c.writerow(line)
					comp_post+=1
				else:
					writer.writerow(line)
					non_comp_post+=1
		except Exception as e:
			log.warning(f"Error occuredfailed: {e}")
			handle.close()
			handle_c.close()
			input_handle.close()
			writer_stat.writerow(f"Not Completed : {total_post:,} : {comp_post:,} : {non_comp_post:,}")
			log.info(f"Not Completed : {total_post:,} : {comp_post:,} : {non_comp_post:,}")

	handle.close()
	handle_c.close()
	input_handle.close()
	writer_stat.writerow(f"Completed : {total_post:,} : {comp_post:,} : {non_comp_post:,}")
	log.info(f"Complete : {total_post:,} : {comp_post:,} : {non_comp_post:,}")

def preprocess_text(text):
    if pd.isna(text):
        return ""
    text = text.lower()  # Convert to lowercase
    text = re.sub(r'\b\w{1,2}\b', '', text)  # Remove short words (optional)
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    #text = ' '.join([word for word in text.split() if word not in cachedStopWords])
    return text

def classify_comment(pipe,comment):
	try:
		if not comment.strip():  # If the comment is empty after preprocessing
			#log.info(f"returns not sure here")
			return 'not sure'
		candidate_labels = ['computing', 'non-computing']
		result = pipe(comment, candidate_labels)
		output = result.get("labels")[0] + "   " +str(result.get("scores")[0])
		#log.info(f"Output : {output}")
		return result.get("labels")[0]
	except Exception as e:
		log.info(f"exception is here: {e}")
		return 'not sure'

if __name__ == "__main__":
	input_files = []
	if os.path.isdir(input_file):
		if not os.path.exists(output_file):
			os.makedirs(output_file)
		if not os.path.exists(output_file_c):
			os.makedirs(output_file_c)
		for file in os.listdir(input_file):
			if not os.path.isdir(file) and file.endswith(".csv"):
				input_name = os.path.splitext(os.path.splitext(os.path.basename(file))[0])[0]
				input_files.append((os.path.join(input_file, file), os.path.join(output_file, input_name),os.path.join(output_file_c, input_name)))
	else:
		input_files.append((input_file, output_file, output_file_c))
	log.info(f"Processing {len(input_files)} files")
	token= ""#hugging face token
	pipe = pipeline("zero-shot-classification", model="facebook/bart-large-mnli",token=token)
	#pipe = pipeline("text-classification", model="distilbert/distilbert-base-uncased-finetuned-sst-2-english",num_labels=2)
	for file_in, file_out, file_out_c in input_files:
		process_file(pipe,file_in, file_out, file_out_c)
	handle_stat.close()



