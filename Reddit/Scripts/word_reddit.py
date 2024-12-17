from wordcloud import WordCloud
from collections import Counter
from smart_open import open  # for transparently opening remote files
import os
import csv
import json
import re
input_file = r"/Users/popoolso/Desktop/git/intern/Forums/cleaned/subreddit"
#csv.field_size_limit(max_int)
#stop =['dirtypenpals','KikRoleplay','DirtyKIKRoleplay']
def getFiles():
	input_files = []
	if os.path.isdir(input_file):#
		for file in os.listdir(input_file):
			if not os.path.isdir(file) and file.endswith(".csv"):
				input_name = os.path.splitext(os.path.splitext(os.path.basename(file))[0])[0]
				input_files.append((os.path.join(input_file, file)))
	else:
		input_files.append((input_file))
	return input_files


	
def createWordCloud():
    #input_handle = open(input_file, mode='r',encoding='UTF-8',newline='',errors='ignore')
    wc = WordCloud(stopwords=None)
    """
    wc.stopwords.add('intern')
    wc.stopwords.add('interns')
    wc.stopwords.add('internship')
    wc.stopwords.add('internships')
    wc.stopwords.add('use')
    wc.stopwords.add('https')
    """
    
    counts_all = Counter()
    for files in getFiles():
        print(files)
        for line in csv.reader(open(files, mode='r',encoding='UTF-8',newline='',errors='ignore')):
            counts_line = wc.process_text(line[5])
            counts_all.update(counts_line)
    """
    with open('path/to/file.txt', 'r') as f:
        for line in f:  # Here you can also use the Cursor
           counts_line = wc.process_text(line)
           counts_all.update(counts_line)
    """
    
    wc.generate_from_frequencies(counts_all)
    wc.to_file('wc2024_red_clean2.png')
    print(len(counts_all))
    print(counts_all.most_common())
    json.dump( counts_all, open( "wc_data_24_red_clean2.json", 'w' ) )
    
    # Read data from file:
    #data = json.load( open( "file_name.json" ) )
    #input_handle.close()  
def readFrom():
	counts=Counter()
	with open( "wc_data_24_red2.json", 'r' ) as file:
		count = json.loads(file.read())
		counts.update(count)
	print(counts.most_common(200))
createWordCloud()
def combineFiles():
	handle = open(r"/Users/popoolso/Desktop/git/intern/Forums/cleaned/total.csv", 'w', encoding='UTF-8', newline='')
	writer = csv.writer(handle)
	handle_t = open(r"/Users/popoolso/Desktop/git/intern/Forums/cleaned/title.csv", 'w', encoding='UTF-8', newline='')
	writer_t = csv.writer(handle_t)	
	handle_b = open(r"/Users/popoolso/Desktop/git/intern/Forums/cleaned/body.csv", 'w', encoding='UTF-8', newline='')
	writer_b = csv.writer(handle_b)		
	file =  open(r"/Users/popoolso/Desktop/git/intern/Forums/cleaned/total.txt", 'w', encoding='UTF-8', newline='')
	file_t =  open(r"/Users/popoolso/Desktop/git/intern/Forums/cleaned/total_t.txt", 'w', encoding='UTF-8', newline='')
	
	for files in getFiles():
		print(files)
		for line in csv.reader(open(files, mode='r',encoding='UTF-8',newline='',errors='ignore')):
			writer.writerow([line[3],line[-1]])
			writer_t.writerow([line[3]])
			writer_b.writerow(line[-1])
			file.write(line[-1])
			file_t.write(line[3])
	handle.close()
	handle_b.close()
	handle_t.close()
	file.close()
	file_t.close()
#combineFiles()
	