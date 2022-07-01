import psycopg2
from sqlalchemy import false, true

from cross_match import check_same_name, replace_and_split

def clearstring(name):
	seperate = name.split(".")

	formatted = ""
	for word in seperate:
		formatted = formatted + word.strip() + " "

	return formatted	

TITLECODE = "#*"
INDEXCODE = "#i"
ABSTRACTCODE = "#!"
AUTHORCODE = "#@"
YEARCODE = "#t"
VENUECODE = "#c"
CITESCODE = "#%"

DB_HOST_ADDRESS = "localhost" #127.0.0.1
DB_NAME = "database"
DB_USER_NAME = "postgres"
DB_PASSWD = "database"

FILE_NAME = "source.txt"

file = open(FILE_NAME,"r")
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER_NAME, password=DB_PASSWD,host=DB_HOST_ADDRESS)
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS paper cascade")
cur.execute("DROP TABLE IF EXISTS Author cascade")
cur.execute("DROP TABLE IF EXISTS Author_OtherNames cascade")
cur.execute("DROP TABLE IF EXISTS citation cascade")
cur.execute("DROP TABLE IF EXISTS list_authors cascade")
cur.execute("DROP TABLE IF EXISTS written_by cascade")
conn.commit()

cur.execute("""

	CREATE TABLE IF NOT EXISTS Author
	(
	AId INT NOT NULL,
	First_Name Text NOT NULL,
	Last_Name Text NOT NULL,
	Middle_Name Text, 
	PRIMARY KEY (aid)
	);
""")
conn.commit()

cur.execute("""
	CREATE TABLE IF NOT EXISTS paper
	(
	pid INT NOT NULL,
	main_author TEXT NOT NULL,
	main_author_id INT NOT NULL,
	year_of_pub INT NOT NULL,
	paper_title TEXT NOT NULL,
	publication_venue TEXT,
	abstract TEXT NOT NULL,
	FOREIGN KEY (main_author_id) REFERENCES Author(aid),
	PRIMARY KEY (pid)
	);
""")
conn.commit()


cur.execute("""

	CREATE TABLE IF NOT EXISTS list_authors
	(
	author_name TEXT NOT NULL,
	rank_as_per_paper INT NOT NULL,
	aid INT NOT NULL,
	pid INT NOT NULL,
	FOREIGN KEY (pid) REFERENCES paper(pid),
	PRIMARY KEY (author_name,pid)
	);
""")
conn.commit()

cur.execute("""

	CREATE TABLE IF NOT EXISTS written_by
	(
	author_name TEXT NOT NULL,
	rank_as_per_paper INT NOT NULL,
	aid INT NOT NULL,
	pid INT NOT NULL,
	FOREIGN KEY (aid) REFERENCES author(aid),
	FOREIGN KEY (pid) REFERENCES paper(pid),
	PRIMARY KEY (author_name,pid)
	);
""")
conn.commit()

cur.execute("""

	CREATE TABLE IF NOT EXISTS Temp_Citation
	(
	CId INT NOT NULL,
	Ref_To INT NOT NULL,
	Ref_By INT NOT NULL,
	PRIMARY KEY (CId)
	);
""")
conn.commit()

cur.execute("""

	CREATE TABLE IF NOT EXISTS citation
	(
	cid INT NOT NULL,
	ref_to INT NOT NULL,
	ref_by INT NOT NULL,
	FOREIGN KEY (ref_by) REFERENCES paper(pid),
	FOREIGN KEY (ref_to) REFERENCES paper(pid),
	PRIMARY KEY (cid)
	);
""")
conn.commit()


cur.execute("""

	CREATE TABLE IF NOT EXISTS Author_OtherNames
	(
	aid INT NOT NULL,
	Name Text NOT NULL,
	FOREIGN KEY (aid) REFERENCES Author(aid),
	PRIMARY KEY (Name)
	);
""")
conn.commit()
s=" "
cid=1
aid=1
temp_id=0
rank = 1
is_present = false
title = ""
authors = []
year_of_pub= ""
venue_of_pub = ""
cites = []
abstract = ""
paper_id = ""
cites_data = []
#cur.execute("INSERT INTO Author (aid,First_Name,Last_Name,Middle_Name) VALUES(0,'a','b','c') ON CONFLICT DO NOTHING")
while s !="":
	s = file.readline()
	code = s[0:2]
	info = s[2:]
	if s!="\n" and s!='':
		if code == TITLECODE:
			title = info
		elif code == INDEXCODE:
			paper_id = s[6:]
		elif code == CITESCODE:
			cites.append(info)
		elif code == AUTHORCODE:
			authors = info.split(",")
		elif code == ABSTRACTCODE:
			abstract = info
		elif code == VENUECODE:
			venue_of_pub = info
		elif code == YEARCODE:
			year_of_pub = info
	elif s == '' or s == '\n':
		if(paper_id!=""):			
			
			no_rep_authors = []
			# to remove duplicates in author name
			for author in authors:
				if author not in no_rep_authors:
					author = clearstring(author)
					no_rep_authors.append(author.strip())
			for author in no_rep_authors:
				if author!='' and (author.count(' ')>=1 or author.count('.')>=1):
				
					if aid==1:
						temp_result=replace_and_split(author)
						middle_name=str("")
						for name in temp_result[2:]:
							middle_name=middle_name+str(name)
						if middle_name=="":
							middle_name="NULL"
							#cur.execute("INSERT INTO Author (aid,First_Name,Last_Name) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",(int(aid),temp_result[0],temp_result[1]))
						
						cur.execute("INSERT INTO Author (aid,First_Name,Last_Name,Middle_Name) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",(int(aid),temp_result[0],temp_result[1],middle_name))
						temp_id=aid
						aid+=1
					else:
						author_query=[]
						cur.execute("select * from author;")
						author_query=cur.fetchall()
						for an_author in author_query:
							temp_name=[]
							temp_name.append(an_author[1])
							temp_name.append(an_author[2])
							if an_author[3]!="" and an_author[3]!="None":
								temp_name.append(an_author[3])
							temp_author=[]
							temp_author=replace_and_split(author)
							temp_result=check_same_name(temp_name,temp_author)
							is_present=temp_result[0]
							if is_present==true:
								middle_name=an_author[3]
								if middle_name=='':
									middle_name="NULL"
								cur.execute("UPDATE Author set first_name = %s,last_name = %s,middle_name = %s WHERE aid = %s ",(temp_result[1][0],temp_result[1][1],middle_name,int(an_author[0])))
								cur.execute("INSERT INTO Author_OtherNames (Name,aid) VALUES(%s,%s) ON CONFLICT DO NOTHING",(author,an_author[0]))
								temp_id=an_author[0]
								break
						if is_present==false:
							temp_result=replace_and_split(author)
							middle_name=str("")
							for name in temp_result[2:]:
								middle_name=middle_name+str(name)+str(" ")
							if middle_name=="":
								middle_name="NULL"
							cur.execute("INSERT INTO Author (aid,First_Name,Last_Name,Middle_Name) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",(int(aid),temp_result[0],temp_result[1],middle_name))
							temp_id=aid
							aid+=1
					if rank==1:
						if venue_of_pub == "" or venue_of_pub=="\n":
							cur.execute("INSERT INTO paper (pid,paper_title,abstract,main_author,main_author_id,year_of_pub,publication_venue) VALUES(%s,%s,%s,%s,%s,%s,NULL) ON CONFLICT DO NOTHING",(int(paper_id),title,abstract,authors[0],temp_id,int(year_of_pub)))
						else:
							cur.execute("INSERT INTO paper (pid,paper_title,abstract,main_author,main_author_id,year_of_pub,publication_venue) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",(int(paper_id),title,abstract,authors[0],temp_id,int(year_of_pub),venue_of_pub))

					cur.execute("INSERT INTO written_by (author_name,rank_as_per_paper,pid,aid) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",(author,rank,int(paper_id),int(temp_id)))
					rank+=1
				else:
					if author=='' or author=='\n':
						author="NULL"
					if aid==1:
						
							#cur.execute("INSERT INTO Author (aid,First_Name,Last_Name) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",(int(aid),temp_result[0],temp_result[1]))
						
						cur.execute("INSERT INTO Author (aid,First_Name,Last_Name,Middle_Name) VALUES(%s,%s,NULL,NULL) ON CONFLICT DO NOTHING",(int(aid),author))
						temp_id=aid
						aid+=1
					else:
						author_query=[]
						cur.execute("select * from author;")
						author_query=cur.fetchall()
						for an_author in author_query:
							temp_name=[]
							temp_name.append(an_author[1])
							temp_name.append(an_author[2])
							if an_author[3]!="" and an_author[3]!="None":
								temp_name.append(an_author[3])
							temp_author=[]
							temp_author=replace_and_split(author)
							temp_result=check_same_name(temp_name,temp_author)
							is_present=temp_result[0]
							if is_present==true:
								middle_name=an_author[3]
								if middle_name=='':
									middle_name="NULL"
								cur.execute("UPDATE Author set first_name = %s,last_name = %s,middle_name = %s WHERE aid = %s ",(temp_result[1][0],temp_result[1][1],middle_name,int(an_author[0])))
								cur.execute("INSERT INTO Author_OtherNames (Name,aid) VALUES(%s,%s) ON CONFLICT DO NOTHING",(author,an_author[0]))
								temp_id=an_author[0]
								break
						if is_present==false:
							temp_result=replace_and_split(author)
							middle_name=str("")
							for name in temp_result[2:]:
								middle_name=middle_name+str(name)
							if middle_name=="":
								middle_name="NULL"
							last_name=str("")
							if len(temp_result)>1:
								last_name=last_name+str(temp_result[1])
							if last_name=="":
								last_name="NULL"
							cur.execute("INSERT INTO Author (aid,First_Name,Last_Name,Middle_Name) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",(int(aid),temp_result[0],last_name,middle_name))
							temp_id=aid
							aid+=1
					if rank==1:
						if venue_of_pub == "" or venue_of_pub=="\n":
							cur.execute("INSERT INTO paper (pid,paper_title,abstract,main_author,main_author_id,year_of_pub,publication_venue) VALUES(%s,%s,%s,%s,%s,%s,NULL) ON CONFLICT DO NOTHING",(int(paper_id),title,abstract,authors[0],temp_id,int(year_of_pub)))
						else:
							cur.execute("INSERT INTO paper (pid,paper_title,abstract,main_author,main_author_id,year_of_pub,publication_venue) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",(int(paper_id),title,abstract,authors[0],temp_id,int(year_of_pub),venue_of_pub))

					cur.execute("INSERT INTO written_by (author_name,rank_as_per_paper,pid,aid) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",(author,rank,int(paper_id),int(temp_id)))
					rank+=1
				
				is_present=false
		
			#if len(cites)==0:
				#cur.execute("INSERT INTO Temp_Citation (CId,Ref_To,Ref_By) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",(cid,"NULL",int(paper_id)))
			for cite in cites:
				cur.execute("INSERT INTO Temp_Citation (CId,Ref_To,Ref_By) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",(cid,cite,int(paper_id)))
				cid+=1

		title = ""
		authors = []
		abstract = ""
		paper_id = ""
		year_of_pub = ""
		venue_of_pub = ""
		cites = []
		rank=1
		no_rep_authors=[]

	#s = file.readline()

conn.commit()

cur.execute("INSERT INTO citation (cid,ref_To,ref_By) SELECT CId,Ref_To,Ref_By FROM Temp_Citation")
cur.execute("INSERT INTO list_authors (author_name,rank_as_per_paper,pid,aid) SELECT author_name,rank_as_per_paper,pid,aid FROM written_by")
cur.execute("DROP TABLE Temp_Citation")
conn.commit()
cur.close()
conn.close()
