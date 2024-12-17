from collections import Counter
from smart_open import open  # for transparently opening remote files
import os
import csv
import json
import re
import logging.handlers
input_file = r"/Raw Data"
output_file = r"/subreddit"
output_file_c = r"/non-subreddit"

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
handle_stat = open(r"/stat/stat.csv", 'w', encoding='UTF-8', newline='')
writer_stat = csv.writer(handle_stat)


def process_file(input_file, output_file,output_file_c):
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

	clean = [('cscareerquestions', 30209), ('csMajors', 12017), ('chanceme', 5177), ('ITCareerQuestions', 5026), ('jobs', 4648), ('careerguidance', 4033), ('learnprogramming', 3993), ('Advice', 3805), ('gradadmissions', 3776), ('EngineeringStudents', 3594), ('ApplyingToCollege', 3272), ('FinancialCareers', 2348), ('internships', 2153), ('resumes', 2142), ('developersIndia', 1886), ('college', 1535), ('cscareerquestionsEU', 1525), ('AskReddit', 1383), ('AskEngineers', 1257), ('careeradvice', 1157), ('sysadmin', 1122),  ('MBA', 1026), ('datascience', 996), ('DigitalCodeSELL', 965), ('findapath', 912), ('MSCS', 879), ('collegeresults', 860), ('nosleep', 846), ('ECE', 838), ('webdev', 817), ('IWantOut', 806), ('SuicideWatch', 776), ('legaladvice', 775), ('berkeley', 755), ('Big4', 755), ('labtec901', 745), ('GradSchool', 712), ('UIUC', 700), ('forhire', 692), ('buildapc', 681), ('uwaterloo', 626), ('UofT', 603), ('cybersecurity', 578), ('ADHD', 573), ('jobbit', 560), ('talesfromtechsupport', 530), ('india', 526), ('mentalhealth', 507), ('computerscience', 479), ('UMD', 462), ('SGExams', 459), ('gatech', 450), ('antiwork', 449), ('CompTIA', 441), ('ElectricalEngineering', 436), ('DevelEire', 410), ('techsupport', 408), ('engineering', 407), ('Vent', 399), ('tabled', 395), ('EngineeringResumes', 394), ('nus', 391), ('techjobs', 381), ('rutgers', 375), ('gamedev', 372), ('germany', 365), ('utdallas', 364), ('Indian_Academia', 364), ('AskProgramming', 363), ('UCSD', 359), ('gis', 357), ('cscareerquestionsCAD', 351), ('IntltoUSA', 345), ('selfimprovement', 342), ('tifu', 337), ('ComputerEngineering', 334), ('bioinformatics', 330), ('learnpython', 328), ('PinoyProgrammer', 326), ('jobsUSAimmigration', 322), ('OMSCS', 309), ('AskAcademia', 302), ('learnmachinelearning', 299), ('programare', 299), ('dataengineering', 296), ('needadvice', 295), ('ucf', 295), ('Purdue', 290), ('SuggestALaptop', 289), ('startups', 288), ('OSUOnlineCS', 286), ('phcareers', 284), ('ucla', 283), ('HFY', 279),  ('AskComputerScience', 277), ('excel', 276), ('consulting', 276), ('UCI', 274), ('PersonalFinanceCanada', 273), ('embedded', 265), ('Entrepreneur', 262), ('AskHR', 262), ('rant', 260), ('Resume', 260), ('Teachers', 259), ('NYCjobs', 256), ('socialskills', 253), ('ReverseChanceMe', 252), ('devops', 250), ('TransferToTop25', 243), ('AsianParentStories', 239), ('IBM', 232), ('quant', 232), ('AskNetsec', 229), ('unsw', 229), ('uofm', 228), ('Cornell', 225), ('dataanalysis', 224), ('compsci', 220), ('askSingapore', 219), ('socialanxiety', 218), ('ufl', 218), ('udub', 215), ('UTAustin', 214), ('GetEmployed', 212), ('Career_Advice', 211), ('InformationTechnology', 209), ('Concordia', 207), ('cscareerquestionsOCE', 207), ('pcmasterrace', 206), ('UniUK', 206), ('LifeAdvice', 205),  ('mcgill', 203), ('Nepal', 199), ('Indians_StudyAbroad', 199), ('PwC', 197), ('therapists', 197), ('work', 192), ('UCDavis', 190), ('SoftwareEngineering', 190)]
	new_list=['jobsfordevelopers','typescript','devnep','BostonU','Harvard','jobsearch','CollegeRant','Frontend','Information_Security',
	   'PennStateUniversity','softwarearchitecture','Temple','Cisco','CSEducation','RemoteJobs']
	for i in clean:
		new_list.append(i[0])
	
	
	for line in csv.reader(input_handle):
		try:
			total_post += 1
			if line[5] in new_list:
				writer.writerow(line)
				comp_post += 1
			
			else:
				writer_c.writerow(line)
				non_comp_post += 1
		except Exception as e:
			log.warning(f"Error occuredfailed: {e}")
			handle.close()
			handle_c.close()
			input_handle.close()
			writer_stat.writerow([input_file,f"Not Completed : {total_post:,} : {comp_post:,} : {non_comp_post:,}"])
			log.info(f"Not Completed : {total_post:,} : {comp_post:,} : {non_comp_post:,}")

	handle.close()
	handle_c.close()
	input_handle.close()
	writer_stat.writerow([input_file,f"Completed : {total_post:,} : {comp_post:,} : {non_comp_post:,}"])
	log.info(f"Complete : {total_post:,} : {comp_post:,} : {non_comp_post:,}")


#readFrom()
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
	for file_in, file_out, file_out_c in input_files:
		process_file(file_in, file_out, file_out_c)
	handle_stat.close()