import os
import boto3
import s3fs
import json
import csv
import psycopg2
import requests
from datetime import datetime
from base64 import b64decode
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

db_host = os.environ['dbhost']
db_name = os.environ['dbname']
db_user = os.environ['dbuser']
ENCR_DB_PASS = os.environ['dbpassword']
db_password_bytes = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCR_DB_PASS))['Plaintext']
db_password = db_password_bytes.decode()
fd_domain = os.environ['fddomain']
ENCR_FD_API_KEY = os.environ['fdapikey']
fd_api_key_bytes = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCR_FD_API_KEY))['Plaintext']
fd_api_key = fd_api_key_bytes.decode()
ENCR_FD_PASS = os.environ['fdpassword']
fd_password_bytes = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCR_FD_PASS))['Plaintext']
fd_password = fd_password_bytes.decode()
input_directory = 'input'
output_directory = 'output'

s3 = s3fs.S3FileSystem(anon=False)

class Ticket:
            def __init__(self, ticket_id, customer_id, customer_name, current_status, ttwa_urgent, sla_urgent, sl_urgent, ttwa_high, sla_high, sl_high, ttwa_medium, sla_medium, sl_medium, ttwa_low, sla_low, sl_low, bizcrit_lost_reason, invalid_bizcrit, invalid_bizcrit_reason, resolved_last_datetime, created_datetime):
                self.ticket_id = ticket_id
                self.customer_id = customer_id
                self.customer_name = customer_name
                self.current_status = current_status
                self.ttwa_urgent = ttwa_urgent
                self.sla_urgent = sla_urgent
                self.sl_urgent = sl_urgent
                self.ttwa_high = ttwa_high
                self.sla_high = sla_high
                self.sl_high = sl_high
                self.ttwa_medium = ttwa_medium
                self.sla_medium = sla_medium
                self.sl_medium = sl_medium
                self.ttwa_low = ttwa_low
                self.sla_low = sla_low
                self.sl_low = sl_low
                self.bizcrit_lost_reason = bizcrit_lost_reason
                self.invalid_bizcrit = invalid_bizcrit
                self.invalid_bizcrit_reason = invalid_bizcrit_reason
                self.resolved_last_datetime = resolved_last_datetime
                self.created_datetime = created_datetime

def parse_csv(input_file):
    with s3.open(input_file, 'r') as file:
        reader = csv.reader(file)
        data = list(reader)
    return(data)

def get_ttwa_or_sla(value):
    if value == '':
        return None;
    else:
        return(int(value))

def get_sl(ttwa, sla):
    if ttwa == None:
        return(None)
    elif sla == None or ttwa <= sla:
        return(True)
    else:
        return(False)

def format_datetime(timestamp):
    parsed_date = datetime.strptime(timestamp, '%d.%m.%Y %H:%M')
    return(datetime.strftime(parsed_date, '%Y-%m-%d %H:%M'))

def get_data_from_fd(ticket_id):
    print("Getting data from freshdesk for ticket " + str(ticket_id))
    fd_data = {"bizcrit_lost_reason": None, "invalid_bizcrit": None, "invalid_bizcrit_reason": None}
    try:
        request = requests.get("https://" + fd_domain + ".freshdesk.com/api/v2/tickets/" + str(ticket_id), auth = (fd_api_key, fd_password))
        response = json.loads(request.content)
        if request.status_code != 200:
            raise Exception("The error occurred calling FD API: " + (str(request.status_code) + ", " + response["message"]))
    except Exception:
        print("Error occurred calling Freshdesk API: ", e)
    fd_data["bizcrit_lost_reason"] = response["custom_fields"]["cf_bizcrit_lost_reason"]
    fd_data["invalid_bizcrit"] = response["custom_fields"]["cf_invalid_bizcrit"]
    fd_data["invalid_bizcrit_reason"] = response["custom_fields"]["cf_invalid_bizcrit_reason"]
    return(fd_data)

def create_tickets_list(data):
    tickets = []
    for i in range(1, len(data)):
            ticket = Ticket(int(data[i][0]), data[i][1], data[i][2], data[i][3], None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
            ticket.sla_urgent = get_ttwa_or_sla(data[i][5])
            ticket.sla_high = get_ttwa_or_sla(data[i][7])
            ticket.sla_medium = get_ttwa_or_sla(data[i][9])
            ticket.sla_low = get_ttwa_or_sla(data[i][11])
            ticket.ttwa_urgent = get_ttwa_or_sla(data[i][4])
            ticket.ttwa_high = get_ttwa_or_sla(data[i][6])
            ticket.ttwa_medium = get_ttwa_or_sla(data[i][8])
            ticket.ttwa_low = get_ttwa_or_sla(data[i][10])
            ticket.sl_urgent = get_sl(ticket.ttwa_urgent, ticket.sla_urgent)
            ticket.sl_high = get_sl(ticket.ttwa_high, ticket.sla_high)
            ticket.sl_medium = get_sl(ticket.ttwa_medium, ticket.sla_medium)
            ticket.sl_low = get_sl(ticket.ttwa_low, ticket.sla_low)
            fd_data = get_data_from_fd(ticket.ticket_id)
            ticket.bizcrit_lost_reason = fd_data["bizcrit_lost_reason"]
            ticket.invalid_bizcrit = fd_data["invalid_bizcrit"]
            ticket.invalid_bizcrit_reason = fd_data["invalid_bizcrit_reason"]
            ticket.resolved_last_datetime = format_datetime(data[i][12])
            ticket.created_datetime = format_datetime(data[i][13])
            tickets.append(ticket)
    return(tickets)

def insert_into_db(tickets):
    print("Initiating DB connection...")
    conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host)
    curr = conn.cursor()
    for i in range(len(tickets)):
        print("Inserting data for ticket " + str(tickets[i].ticket_id))
        curr.execute("""INSERT INTO ttwa (ticket_id, customer_id, customer_name, current_status, ttwa_urgent, sla_urgent, sl_urgent, ttwa_high, sla_high, sl_high, ttwa_medium, sla_medium, sl_medium, ttwa_low, sla_low, sl_low, resolved_last_datetime, created_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (ticket_id) DO UPDATE SET current_status = %s, ttwa_urgent = %s, sla_urgent = %s, sl_urgent = %s, ttwa_high = %s, sla_high = %s, sl_high = %s, ttwa_medium = %s, sla_medium = %s, sl_medium = %s, ttwa_low = %s, sla_low = %s, sl_low = %s, resolved_last_datetime = %s;""", (tickets[i].ticket_id, tickets[i].customer_id, tickets[i].customer_name, tickets[i].current_status, tickets[i].ttwa_urgent, tickets[i].sla_urgent, tickets[i].sl_urgent, tickets[i].ttwa_high, tickets[i].sla_high, tickets[i].sl_high, tickets[i].ttwa_medium, tickets[i].sla_medium, tickets[i].sl_medium, tickets[i].ttwa_low, tickets[i].sla_low, tickets[i].sl_low, tickets[i].resolved_last_datetime, tickets[i].created_datetime, tickets[i].current_status, tickets[i].ttwa_urgent, tickets[i].sla_urgent, tickets[i].sl_urgent, tickets[i].ttwa_high, tickets[i].sla_high, tickets[i].sl_high, tickets[i].ttwa_medium, tickets[i].sla_medium, tickets[i].sl_medium, tickets[i].ttwa_low, tickets[i].sla_low, tickets[i].sl_low, tickets[i].resolved_last_datetime))
        conn.commit()
    curr.close()
    conn.close()
    return(None)

def write_csv(output_file, tickets):
    print("Writing csv...")
    header = ['Ticket ID', 'Customer ID', 'Customer Name', 'Current Status', 'TTWA Urgent', 'SLA Urgent', 'SL Urgent', 'TTWA High', 'SLA High', 'SL High', 'TTWA Medium', 'SLA Medium', 'SL Medium', 'TTWA Low', 'SLA Low', 'SL Low', 'Bizcrit Lost Reason', 'Invalid Bizcrit', 'Invalid Bizcrit Reason', 'Resolved Last Date Time', 'Created Date Time']
    with s3.open(output_file, 'w') as file:
        writer = csv.writer(file, delimiter = ',')
        writer.writerow(header)
        for i in range(len(tickets)):
            row = [tickets[i].ticket_id, tickets[i].customer_id, tickets[i].customer_name, tickets[i].current_status, tickets[i].ttwa_urgent, tickets[i].sla_urgent, tickets[i].sl_urgent, tickets[i].ttwa_high, tickets[i].sla_high, tickets[i].sl_high, tickets[i].ttwa_medium, tickets[i].sla_medium, tickets[i].sl_medium, tickets[i].ttwa_low, tickets[i].sla_low, tickets[i].sl_low, tickets[i].bizcrit_lost_reason, tickets[i].invalid_bizcrit, tickets[i].invalid_bizcrit_reason, tickets[i].resolved_last_datetime, tickets[i].created_datetime]
            writer.writerow(row)
    return(None)

def send_email(attachment_file):
    print("Composing the email...")
    sender = os.environ['senderemail']
    recepient = os.environ['recepientemail']
    aws_region = os.environ['awsregion']
    subject = "TTWA Statistics"
    attachment = attachment_file
    BODY_TEXT = "Hello,\r\nPlease see the attached TTWA statistics file."
    BODY_HTML = """\
            <html>
            <head></head>
            <body>
            <p>Hello,</p>
            <p>Please see the attached TTWA statistics file.</p>
            </body>
            </html>
            """
    CHARSET = "utf-8"
    client = boto3.client('ses', region_name=aws_region)
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject 
    msg['From'] = sender 
    msg['To'] = recepient
    msg_body = MIMEMultipart('alternative')
    textpart = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
    htmlpart = MIMEText(BODY_HTML.encode(CHARSET), 'html', CHARSET)
    msg_body.attach(textpart)
    msg_body.attach(htmlpart)
    att = MIMEApplication(s3.open(attachment, 'rb').read())
    att.add_header('Content-Disposition', 'ttwa_statistics.csv', filename=os.path.basename(attachment))
    msg.attach(msg_body)
    msg.attach(att)
    print("Sending the email...")

    try:
        response = client.send_raw_email(Source=sender, Destinations=[recepient], RawMessage={'Data':msg.as_string(),})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID: " + response['MessageId'])

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        input_file = os.path.join(bucket, key)
        print("Input file " + input_file)
        output_path = os.path.join(bucket, output_directory, os.path.basename(key))
        print("Output file " + output_path)
        print("Reading csv...")
        data = parse_csv(input_file)
        print("Collecting tickets data...")
        tickets = create_tickets_list(data)
        print("Inserting data into db...")
        insert_into_db(tickets)
        write_csv(output_path, tickets)
        send_email(output_path)
        print("Data proccessing completed successfully. Removing files...")
        s3.rm(input_file)
        s3.rm(output_path)
    return(None)  
