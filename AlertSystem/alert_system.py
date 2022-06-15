import pandas as pd
from datetime import date
from ftplib import FTP
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Alert:
    def set_mail_connection(self,username,password,host_name = "email-smtp.us-east-1.amazonaws.com",port_num = 587):
        """
        Establishes a connection with the SMTP server and sets instance variables mail_server,username,passwrod,host_name, and port_num from function parameters
        """
        self.mail_server = smtplib.SMTP(host=host_name, port=port_num)
        self.mail_server.starttls()
        self.mail_server.login(username, password)
        self.username = username
        self.password = password
        self.host_name = host_name
        self.port_num = port_num
    def reset_mail_connection(self):
        """
        Resets connection to the SMTP Server for instance variable mail_server
        """
        self.mail_server = smtplib.SMTP(host=self.host_name, port=self.port_num)
        self.mail_server.starttls()
        self.mail_server.login(self.username, self.password)
        print("Mail Connection re-established")
    def send_mail(self,to,sender,subject,text_content):
        """
        Sends message over SMTP Connection from set_mail_connection from "sender" to recipient,"to", with subject and text_content
        Params:
            to(str): Email address or addresses separated by commas that email will go to
            sender(str): Email address of the sender
            subject(str): Subject of the email
            text_content(str): Body of the email in HTML
        """
        message = MIMEMultipart()
        message['From'] = sender
        message['To'] = to
        message['Subject'] = subject
        textPart = MIMEText(text_content, 'html')
        message.attach(textPart)
        self.mail_server.send_message(message)
        self.mail_server.quit()
    def __init__(self,connection_url,ftp_username,ftp_password):
        """
        Constructor for Alert.
        Initializes connection to the FTP
        Params:
            connection_url(str): url for the ftp connection
            ftp_username(str): username for ftp account
            ftp_password(str): password for ftp account
        """
        self.ftp_username = ftp_username
        self.ftp_password = ftp_password
        self.connection_url = connection_url
        self.ftp = FTP(connection_url)
        self.ftp.login(ftp_username,ftp_password)
        print("connected to the FTP")
    def reset_server(self):
        self.ftp = FTP(self.connection_url)
        self.ftp.login(self.ftp_username,self.ftp_password)
    def construct_message(self,df,model,threshold):
        """Given a dataframe, a model and a threshold, construct_message will construct an email message"""
        subject = ""
        if threshold == "5":   
            subject = f"Cold Alert:{model} Has Breached the {threshold} Degree threshold"
        else:
            subject = f"Frost Alert:{model} Has Breached the {threshold} Degree threshold"
        message = """<table> <tr>
                        <th>City</th> <th>Temperature</th>  <th>Date</th> 
                            </tr>
                """
        for i in range(df.shape[0]):
            message += f""""<tr> <td>{df.iloc[i]['CITY']}</td> 
            <td>{df.iloc[i][df.columns[4]]}</td>   <td>{df.iloc[i]['DATE']}</td></tr>"""
        message += "</table>"
        return subject,message
    def check_data(self,data_source,target_date = date.today(),run ="",attemps = 0):
        """
        Params:
            data_source(str): Source that the function shuold pull from. Options are GFS or ECMWF
            target_date(datetime date): date that the function pulls from
            run(str): Op or Ens. If left unspecified, will pull all
            attemps(integer): For error handling only, do not change
        Returns:
            under_zero_df(pandas dataframe): a dataframe that contains the id, station, city, date and min temp for every predicted temp under
        """
        ftp = self.ftp
        ftp.cwd('Forecasts')
        file_list = ftp.nlst()
        today_list = list(filter(lambda x: str(target_date) in x,file_list))
        today_list = list(filter(lambda x: x.split("_")[-1][:2]=="00" and data_source in x and "RAW" in x and run in x,today_list))
        cols = ['WMO','SRC_ID','CITY','DATE','MIN_TEMP']
        under_five_df = pd.DataFrame(columns = cols)
        offset = 0
        for i in today_list:
            with open(i,"wb") as file:
                ftp.retrbinary(f"RETR {i}", file.write)
            curr_df = pd.read_csv(i)
            key = "TMIN_ENSEMBLE_AVG" if "AVG" in i else ("TMIN_OP")
            if len(curr_df[curr_df[key]<5]) != 0:
                temp = curr_df[curr_df[key]<5]
                for j in range(temp.shape[0]):
                    under_five_df.loc[j+offset] = [temp.iloc[j]['WMO'], temp.iloc[j]['SRC_ID'], temp.iloc[j]['CITY'], temp.iloc[j]['DATE'], temp.iloc[j][key]]
                offset += temp.shape[0]
            del curr_df
            os.remove(i)
        under_five_df = under_five_df.sort_values('CITY')
        under_five_df = under_five_df[(under_five_df['MIN_TEMP']>=2) & (under_five_df['MIN_TEMP']<5)]
        under_two_df = under_five_df[under_five_df['MIN_TEMP']<2]
        under_zero_df = under_five_df[under_five_df['MIN_TEMP']<0]
        return under_five_df,under_two_df,under_zero_df,today_list
   
       