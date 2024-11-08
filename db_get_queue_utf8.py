'''
This code is used to format ticket data so they are ready for finetuning. 
Only tickets can be decoded with utf-8 are used.
The output example is formated to be compatible with DeepSpeed-Chat pipeline.
'''

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import re
import mysql.connector
import os

# Read in email data from sql databse
engine = create_engine('mysql+pymysql://root:root@localhost/rt5', pool_size=10, max_overflow=20)
table = pd.read_sql_table("Attachments", con=engine)
engine.dispose()

print(f"# emails in total: {len(table)}")  

columns = table.columns
no_error, decode_errors = [], []
for idx, series in table.iterrows():
    # Emails that have "AutoReply" and "Resolved" in their subjects are useless 
    if len(series["Subject"]) > 0 and "AutoReply" not in series["Subject"] and "Resolved" not in series["Subject"]:
        try:
            emails = series["Content"].decode("utf-8")
            no_error.append(table.iloc[idx])
        except:
            decode_errors.append(table.iloc[idx])

no_error = pd.DataFrame(no_error, columns=columns).reset_index()
decode_errors = pd.DataFrame(decode_errors, columns=columns).reset_index()

print(f"# emails of pure texts: {len(no_error)}")
print(f"# emails that have utf-8 decode error: {len(decode_errors)}")

# Group tickets with same ticket numbers together
from collections import defaultdict
tickets = defaultdict(list)
for idx, row in no_error.iterrows():
    # Reconstruct the header to dict
    header_split = row["Headers"].split("\n")
    header_dict = {}
    for key_val in header_split:
        splits = key_val.split(": ")
        header_key = splits[0]
        val = ": ".join(splits[1:])
        if len(header_key) == 0:
            continue
        header_dict[header_key]=val    
    if "X-RT-Ticket" not in header_dict.keys():
        continue
    temp_str = ""
    for line in row["Content"].decode("utf-8").split("\n"):
        if len(line) > 0:
            temp_str += "" if line[0] == ">" else (line+"\n")
    header_dict["Email_Content"] = temp_str
    ticket_key = header_dict["X-RT-Ticket"]
    # Simplest deduplication method: check if Content are identical
    if len(tickets[ticket_key]) != 0:
        prev_email = tickets[ticket_key][-1]["Email_Content"]
        cur_email = header_dict["Email_Content"]
        # skip if identical
        if  prev_email == cur_email:
            continue
        # otherwise, keep the one with longer length
        elif prev_email in cur_email:
            tickets[ticket_key].pop()
        elif cur_email in prev_email:
            continue
    tickets[ticket_key].append(header_dict)

print(f"Number of tickets: {len(tickets)}")


stand_alone_emails = {}
stand_alone_keys = []
for key, val in tickets.items():
    if len(val) == 1:
        stand_alone_keys.append(key)

for key in stand_alone_keys:
    stand_alone_emails[key] = tickets.pop(key)

print(f"Total number of stand alone emails: {len(stand_alone_emails)}/{len(tickets)+len(stand_alone_emails)}")



tickets_keys = tickets.keys()
forwarded_keys = []
for key, ticket in tickets.items():
    found = False            
    for email in ticket:
        if not isinstance(email, dict):
            continue        
        email_splited = email['Email_Content'].split('\n')
        for line in email_splited:
            if re.search("look forward to", line):
                continue
            if re.search("(forwarded|forwarding|forward) .*? to .*? (Queue|group|team| Chameleon | High Performance Computing | Technology Infrastructure | Data Intensive Computing | Security | Visualization | DesignSafe-ci | Accounting | Agave | Advanced Computing Interfaces | Life Sciences | Advanced Computing Systems | SD2E | TRADES | Cloud and Interactive Computing | Dell Medical School | Web & Mobile Apps | TUP | Frontera | Epic | NSO | Designsafe-pub-feedback | EPIC-CyberRange | 3DEM | Accounts | Allocations | Citizenship | Feature-Requests | Machine Learning | MFA | PDATA |)", line, re.IGNORECASE):  
                found = True
                break
        if found:
            forwarded_keys.append(key)
            break
            
print(len(tickets), len(forwarded_keys))

tickets_cleaned = {}
for key, item in tickets.items():
    if key not in forwarded_keys:
        tickets_cleaned[key] = item

print(len(tickets), len(tickets_cleaned))


def sanity_check(email_splited):
    for line in email_splited:
        if re.search(r"This ticket is being set to resolved.", line) or re.search(r"This transaction appears to have no content", line) or re.search(r"A ticket has been transferred to .* Queue.", line) or re.search(r"Ticket resolved", line) or re.search(r"Marking as resolved", line) or re.search(r"A ticket has been assigned to you", line) or re.search(r"I will ask one of our team members to take a look at this.", line) or re.search(r"resolved", line):
            return False
    return True

def get_category(email_splited):
    for line in email_splited:
        if "[Category]" in line:
            return line.replace("[Category]", "")
    return None


def get_queue(email_splited):    
    Q_list = [' Chameleon ', ' High Performance Computing ', ' Technology Infrastructure ', ' Data Intensive Computing ', ' Security ', ' Visualization ', ' DesignSafe-ci ', ' Accounting ', ' Agave ', ' Advanced Computing Interfaces ', ' Life Sciences ', ' Advanced Computing Systems ', ' SD2E ', ' TRADES ', ' Cloud and Interactive Computing ', ' Dell Medical School ', ' Web & Mobile Apps ', ' TUP ', ' Frontera ', ' Epic ', ' NSO ', ' Designsafe-pub-feedback ', ' EPIC-CyberRange ', ' 3DEM ', ' Accounts ', ' Allocations ', ' Citizenship ', ' Feature-Requests ', ' Machine Learning ', ' MFA ', ' PDATA ']
    for line in email_splited:
        if re.search(r"A ticket has been created in the .*? Queue.", line):
            return " ".join(line.split(" ")[7:-1])
        if re.search(r"A ticket has been transferred to the .*? Queue.", line):
            return " ".join(line.split(" ")[7:-1])
        if re.search(r"Queue changed from .*? to .*?", line):
            return line.split("to")[1]
    for line in email_splited:
        for q in Q_list:
            if q in line:
                return q[1:-1]
    return None

# Apply the operations above to all tickets
from collections import Counter
Q_cnt = Counter()
question_answer_pairs = []
for key, ticket in tickets_cleaned.items():
    cleaned_emails = []
    category, queue = "", ""
    for i, email in enumerate(ticket):
        if not isinstance(email, dict):
            continue
        cleaned_lines = []
        email_splited = email['Email_Content'].split('\n')  
        if i == 0:
            category = get_category(email_splited)
        queue_info = get_queue(email_splited) 
        if not queue or queue_info:
            queue = queue_info
        if not sanity_check(email_splited):
            continue
        for line in email_splited:
            # Must break the loop if the next line contains history email conversation
            if re.search(r"Original Message", line):
                break
            if line.strip() != "" and (not re.search(r"On .*? via .*?", line)) and (not re.search(r"On .*? wrote:$", line)) and (not re.search(r'Subscribe to user news:', line)) and (not re.search(r'Transaction: |Queue: |Subject: |Owner: |Requestors:| Date: |Status: |Comment by: |Full name: |Phone: |Email: |Comments/Feedback:', line)) and (not re.search('\[.*?\]', line)) and (not re.search(r'Ticket \<.*?\>', line)) and (not re.search(r'Request .*? was acted upon.$', line)) and (not re.search(r'A ticket has been created in the .* Queue.', line)):
                cleaned_lines.append(line.replace("\r", ""))   
        if len(cleaned_lines) != 0:
            cleaned_emails.append("\n".join(cleaned_lines))
    # Avoid the case where tickets are do not have any response.    
    if(len(cleaned_emails)) < 2:
        continue    
    Q_cnt[queue] += 1
    if not queue:
        print(cleaned_emails)
    history = ""
    for i in range(len(cleaned_emails)):
        if i % 2 == 1:
            pair = {"category": category,
                    "queue": queue,
                    "prompt": history + " Assistant:", 
                    "chosen":  " " + cleaned_emails[i],
                    "rejected": " Please submit a ticket through the portal: https://tacc.utexas.edu/portal/dashboard. ",}
            question_answer_pairs.append(pair)
        if i % 2 == 0:
            history += " Human: " + cleaned_emails[i]
        else:
            history += " Assistant: " + cleaned_emails[i]

for i in range(12):
    question_answer_pairs.pop(0)

print(len(question_answer_pairs))
print(f'Queue counting results {Q_cnt}')


Ds_qa_pair = []
for pair in question_answer_pairs:
    if pair['queue'] == 'DesignSafe-ci':
        Ds_qa_pair.append(pair)

import random
random.shuffle(Ds_qa_pair)
num_pairs = len(Ds_qa_pair)
train_eval_proportion = 0.9
train_dataset = Ds_qa_pair[:int(num_pairs * train_eval_proportion)]
eval_dataset = Ds_qa_pair[int(num_pairs * train_eval_proportion):]

import json
with open("data/24ds_train_utf8.json", 'w') as f:
    json.dump(train_dataset, f)
with open("data/24ds_eval_utf8.json", 'w') as f:
    json.dump(eval_dataset, f)

# # Processing emails with image
# for idx, row in decode_errors.iterrows():
#     if "multipart/alternative" in row["Headers"]:
#         print(row["Headers"])
#         break