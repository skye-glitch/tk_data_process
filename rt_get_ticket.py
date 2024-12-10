'''
This code is used to format ticket data so they are ready for finetuning. 
The output example is formated to be compatible with DeepSpeed-Chat pipeline.
'''
import pandas as pd
import re
import os
import rt
from requests.auth import HTTPBasicAuth
from tup_services import settings
from tup_services.tickets.models import (
    Ticket,
    TicketHistoryItem,
    TicketAttachment,
    TStatus,
)
from fastapi.exceptions import HTTPException
import copy

def get_tickets_client() -> rt.Rt:
    """Instantiate an RT client using credentials from settings."""
    client = rt.Rt(
        settings.RT_HOST,
        settings.RT_UN,
        settings.RT_PW,
        http_auth=HTTPBasicAuth(settings.RT_UN, settings.RT_PW),
    )
    client.login()
    return client

def format_history(item: TicketHistoryItem) -> TicketHistoryItem:
    if item.Type == "Status":
        item.Content = item.Description
    last_line = (item.Content or "\n").splitlines()[-1]
    search_result = re.search(r"\[Reply submitted on behalf of (.*?)\]", last_line)
    if search_result:
        item.Creator = search_result.group(1)
    return item

def fetch_ticket_history(ticket_id, client) -> list[TicketHistoryItem]:
    """Fetch all history items for a ticket."""
    #client = get_tickets_client()
    history_resp = client.get_history(ticket_id) or []
    history = list(map(TicketHistoryItem.model_validate, history_resp))
    history = list(map(format_history, history))
    return history

def fetch_ticket(ticket_id, client) -> Ticket:
    """Fetch a single ticket by its ID (e.g. '12345')"""
    #client = get_tickets_client()
    resp = client.get_ticket(ticket_id)
    return Ticket.model_validate(resp)

def fetch_ticket_attachment(ticket_id, attachment_id, client) -> TicketAttachment:
    #client = get_tickets_client()
    attachment = client.get_attachment(ticket_id, attachment_id)
    return TicketAttachment.model_validate(attachment)



def sanity_check(email_splited):
    for line in email_splited:
        if re.search(r"This ticket is being set to resolved.", line) or \
        re.search(r"This transaction appears to have no content", line) or \
        re.search(r"A ticket has been transferred to .* Queue.", line) or \
        re.search(r"Ticket resolved", line) or \
        re.search(r"Marking as resolved", line) or \
        re.search(r"A ticket has been assigned to you", line) or \
        re.search(r"I will ask one of our team members to take a look at this.", line) or \
        re.search(r"have the team look at", line) or \
        re.search(r"ask one of our team members to look into this", line) or \
        re.search(r"RTBot", line) or \
        re.search(r"This message has been automatically generated", line) or \
        re.search(r"Responding to this email will re-open the ticket", line) or \
        re.search(r"Your request has been resolved", line) or \
        re.search(r"has been solved", line): 
            return False
    return True

def find_forward_key(email_splited):
    for line in email_splited:
        if re.search("look forward to", line):
            continue
        if re.search("(forwarded|forwarding|forward) .*? to .*? (Queue|group|team| Chameleon | High Performance Computing | Technology Infrastructure | Data Intensive Computing | Security | Visualization | DesignSafe-ci | Accounting | Agave | Advanced Computing Interfaces | Life Sciences | Advanced Computing Systems | SD2E | TRADES | Cloud and Interactive Computing | Dell Medical School | Web & Mobile Apps | TUP | Frontera | Epic | NSO | Designsafe-pub-feedback | EPIC-CyberRange | 3DEM | Accounts | Allocations | Citizenship | Feature-Requests | Machine Learning | MFA | PDATA |)", line, re.IGNORECASE):  
            return True
    return False
            
def form_history_with_speaker(cleaned_emails, question_answer_pairs):
    history = ""
    for i in range(0, len(cleaned_emails), 2):
        if cleaned_emails[i] == "Assistant":
            pair = {"prompt": history + "Assistant:", 
                    "chosen":  " " + cleaned_emails[i + 1],
                    "rejected": " Please submit a ticket through the portal: https://tacc.utexas.edu/portal/dashboard. ",}
            question_answer_pairs.append(pair)
        if cleaned_emails[i] == "Human":
            history += "Human: " + cleaned_emails[i + 1] + '\n'
        else:
            history += "Assistant: " + cleaned_emails[i + 1] + '\n'
    return history

def form_history_without_speaker(cleaned_emails, question_answer_pairs):
    history = ""
    for i in range(0, len(cleaned_emails), 2):
        if i % 4 == 2:
            pair = {"prompt": history + "Assistant:", 
                    "chosen":  " " + cleaned_emails[i + 1],
                    "rejected": " Please submit a ticket through the portal: https://tacc.utexas.edu/portal/dashboard. ",}
            question_answer_pairs.append(pair)
        if i % 4 == 0:
            history += "Human: " + cleaned_emails[i + 1] + '\n'
        else:
            history += "Assistant: " + cleaned_emails[i + 1] + '\n'
    return history

def get_user_email(email_Description, email_content):
    user_email = None
    # marked by <> at the end of content
    if "Ticket created by" in email_Description and "[Opened by]" in email_content:
        try:
            user_email = email_content.split("<")[1].split(">")[0]
        except:
            pass
    elif "Requestor" in email_content:
        #tickets submitted through webpage?
        user_email = email_content.split("(")[1].split(")")[0]
    return user_email

def get_user_name(email_Description, email_content):
    if "rtprod" not in email_Description:
        ticket_creator = email_Description.split()[-1]
    elif "[Opened by]" in email_content:
        ticket_creator = email_content.split("[Opened by] ")[1].split('\n')[0]
    elif "Requestor" in email_content:
        ticket_creator = email_content.split("Requestor: ")[1].split('\n')[0].split('(')[0][:-1]
    else:
        #print(f"cannot find ticket creater user name, set it to rt")
        ticket_creator = "rt"
    return ticket_creator

def seen_before(line, cleaned_emails, legal_name):
    if  re.search(r"For faster response, please message me on Slack", line) or \
        re.search(r"zt-1owto8ayr-NWxjKL00u~BiptwP6Yyomw", line):
        return True
    if re.search(r"Original Message", line) or \
        (re.search(r"HTTP Referer", line) and len(cleaned_emails) > 0 ) or \
        (line.strip() != "" and line.strip().strip('_') == "") or \
        (line.strip() != "" and line.strip().strip('-') == "") or \
        (re.search(r"On (Mon|Tue|Wed|Thu|Fri|Sat|Sun).* (Jan|Feb|Mar|Apr|Mat|Jun|Jul|Aug|Sep|Oct|Nov|Dec).* (\d{1,2}).*(\d4).*wrote:", line)):
            return True
    if ('>' in line and \
        len(cleaned_emails) > 0 and line.strip() != "" and \
        line.strip().strip('>') != ""):
            msg = line.strip().strip('>').strip()
            # if re.search(msg, legal_name, re.IGNORECASE):
            #     return True
            for cleaned_email in cleaned_emails:
                if msg in cleaned_email:
                    return True
    return False

def filter_useful_msg(email_Description, email_content):
    if ("Correspondence added by" in email_Description and \
        "Correspondence added by rtprod" not in email_Description) or \
        "Ticket created by" in email_Description or \
        ("Correspondence added by rtprod" in email_Description and \
        email_content.startswith("[Reply from]")):
        return True
    else:
        return False

def reply_by_email(email_Description):
    if re.search(r"Correspondence added by .*@.*\..*", email_Description):
        return True
    return False

def filter_useful_line(line):
    if line.strip() == "" or \
        line.strip().strip('>').strip() == "" or \
        re.search(r"On .*? via .*?", line) or \
        re.search(r'Subscribe to user news:', line) or \
        re.search(r'Category: |Transaction: |System/Resource: |Requestor: |Queue: |Subject: |Owner: |Requestors:| Date: |Status: |Comment by: |Full name: |Phone: |Email: |Comments/Feedback: |\[Reply from\] |\[Opened by\] |\[Category\] |\[Resource\] |\[HTTP Referer\]', line) or \
        re.search(r'Ticket \<.*?\>', line) or \
        re.search(r'Request .*? was acted upon.$', line) or \
        re.search(r'A ticket has been created in the .* Queue.', line) or \
        re.search(r"This ticket is being set to resolved.", line) or \
        re.search(r"This transaction appears to have no content", line) or \
        re.search(r"A ticket has been transferred to .* Queue.", line) or \
        re.search(r"Ticket resolved", line) or \
        re.search(r"Marking as resolved", line) or \
        re.search(r"A ticket has been assigned to you", line) or \
        re.search(r"I will ask one of our team members to take a look at this.", line) or \
        re.search(r"Status changed from .* to .* by .*", line) or \
        re.search(r".*\[.*\].* reacted to your message:", line):
        return False
    else:
        return True

def get_history(batch_of_tickets, index, question_answer_pairs, client):
    ticket_id = batch_of_tickets[index]['id'].split("/")[1]
    ticket_history = fetch_ticket_history(ticket_id, client)
    cleaned_emails = []
    user_email = "email_placeholder"
    legal_name = "name_placeholder"
    ticket_creator = "rt"
    mark_skip = False
    for i, email in enumerate(ticket_history):
        found = False
        email_content = getattr(email,'Content')
        email_Description = getattr(email, 'Description')
        email_atts = getattr(email, 'Attachments')
        for att in email_atts:
            att_id = att[0]
            try:
                attname = getattr(fetch_ticket_attachment(ticket_id, str(att_id), client), 'Filename')
            except:
                mark_skip = True
                break
            if attname != '':
                mark_skip = True
                break
        if mark_skip:
            break
        try:
            email_content.encode('ascii')
        except:
            # email contains other language
            try:
                email_content.encode('latin-1')
                mark_skip = True
                break
            except:
                pass
        if email_Description == "Comments added by rtbot":
            if "Username:" in email_content:
                ticket_creator = email_content.split("Username:..................... ")[1].split()[0]
                user_email = email_content.split("Email:........................ ")[1].split()[0]
                legal_name = email_content.split("Name:......................... ")[1].split()[0]
        if not filter_useful_msg(email_Description, email_content):
            continue 
        if user_email == "email_placeholder":
            find_email = get_user_email(email_Description, email_content) 
            if find_email != None:
                user_email = find_email
        current_speaker = "Assistant"    
        if "Ticket created by" in email_Description:
            ticket_creator = get_user_name(email_Description, email_content) 
            current_speaker = "Human"
            # http referer starts history unless its the first ticket
            email_content = re.sub(r'(?s).*\[HTTP Referer\]', '', email_content)
        elif ticket_creator in email_Description or user_email in email_Description:
            current_speaker = "Human"
        elif "rtprod" in email_Description and email_content.startswith("[Reply from]"):
            speaker = email_content.split("[Reply from] ")[1].split('\n')[0]
            if speaker == ticket_creator:
                current_speaker = "Human"
        


        email_splited = email_content.split('\n')
        cleaned_lines = []
        # If the ticket is forwarded, it is not a good case to learn from
        if find_forward_key(email_splited):
            break
        # exclude examples with nothing to learn from
        if not sanity_check(email_splited):
            continue
        prev_line = None
        for line in email_splited:
            # Break the loop if the next line contains history email conversation
            if seen_before(line, cleaned_emails, legal_name):
                break
            cur_line = line.replace("\r", "")
            if reply_by_email(email_Description):
                cur_line = cur_line.removeprefix('> ')
            if filter_useful_line(cur_line):
                if prev_line is not None:
                    # some not usefule lines are seperated by \n
                    long_line = prev_line + cur_line
                    # Break the loop if the next line contains history email conversation
                    if seen_before(long_line, cleaned_emails, legal_name):
                        if cleaned_lines[-1] == prev_line:
                            cleaned_lines = cleaned_lines[:-1]
                        break                        
                    if (not filter_useful_line(long_line)):
                        # remove last line from the end of email
                        # and don't add this line into useful info
                        if len(cleaned_lines) != 0 and cleaned_lines[-1] == prev_line:
                            cleaned_lines = cleaned_lines[:-1]
                    else:
                        cleaned_lines.append(cur_line) 
                else:
                    cleaned_lines.append(cur_line) 
            prev_line = cur_line  
        if len(cleaned_lines) != 0:
            cleaned_emails.append(current_speaker)
            cleaned_emails.append("\n".join(cleaned_lines))   
    # Avoid the case where tickets are do not have any response.    
    if(len(cleaned_emails)) < 2:
        mark_skip = True
    
    if not mark_skip:
        if ticket_creator != "rt":
            history = form_history_with_speaker(cleaned_emails, question_answer_pairs)
        else:
            history = form_history_without_speaker(cleaned_emails, question_answer_pairs)
        #print(f"add {len(cleaned_emails)}")
    return 
    
def main(FromDate='2020-01-01',ToDate='2021-01-01', First='False'):
    # Read in tickets with rt
    client = get_tickets_client()
    #batch_of_tickets = client.last_updated(since="2022-08-24",queue="DesignSafe-ci")
    query = f'Created > \'{FromDate}\' AND Created < \'{ToDate}\''
    batch_of_tickets = client.search(Queue="DesignSafe-ci", raw_query=query)
    #batch_of_tickets = client.search(Queue="DesignSafe-ci", raw_query="Created < '2023-08-09' AND Created > '2022-11-01'")
    tkCount = len(batch_of_tickets)
    print(f"# ticket in total: {tkCount}")  
    question_answer_pairs = []
    for _ in range(tkCount):   
        get_history(batch_of_tickets, _, question_answer_pairs, client)   
    client.logout() 


    import random
    random.shuffle(question_answer_pairs)
    num_pairs = len(question_answer_pairs)
    print(f"QA pair number is {num_pairs}")
    train_eval_proportion = 0.9
    train_dataset = question_answer_pairs[:int(num_pairs * train_eval_proportion)]
    eval_dataset = question_answer_pairs[int(num_pairs * train_eval_proportion):]
    return train_dataset, eval_dataset
    # import json
    # if First == 'True':
    #     with open("/data/24ds_train_ascii.json", 'w') as f:
    #         json.dump(train_dataset, f)
    #     with open("/data/24ds_eval_ascii.json", 'w') as f:
    #         json.dump(eval_dataset, f)
    # else:
    #     with open("/data/24ds_train_ascii.json", 'a') as f:
    #         json.dump(train_dataset, f)
    #     with open("/data/24ds_eval_ascii.json", 'a') as f:
    #         json.dump(eval_dataset, f)

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Put in date range to process')
    parser.add_argument('--FromDate', help='Earliest create date, 2020-01-01')
    parser.add_argument('--ToDate', help='Latest create date, 2021-01-01')
    parser.add_argument('--First', help='If this is the first call (will clear out json file)')
    args = parser.parse_args()
    main(FromDate=args.FromDate, ToDate=args.ToDate, First=First)