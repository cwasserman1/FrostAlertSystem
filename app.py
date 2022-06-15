from AlertSystem import alert_system
import pandas as pd
import datetime
import time
from pytz import timezone
from freezegun import freeze_time
df = pd.read_csv('credentials.csv', delimiter="\t", encoding='utf-8')
df = df.set_index(df['Keys'])
ftp_url = df.loc['ftp-connection-url']['Vals']
ftp_username = df.loc['ftp-username']['Vals']
ftp_password = df.loc['ftp-password']['Vals']
email_username = df.loc['email-username']['Vals']
email_password = df.loc['email-password']['Vals']
test_alert = alert_system.Alert(ftp_url,ftp_username,ftp_password)
test_alert.set_mail_connection(email_username,email_password)

Recipient = "cole@sucafina.com,greg@sucafina.com,Ilya@sucafina.com"
under_five_recipient = "cole@sucafina.com,greg@sucafina.com"
Sender = "WeatherAlertSystem_NA@sucafina.com"

curr_hour = None
prev_day = None
gfsop_hit,gfsens_hit,ecmwfens_hit = True,True,True
gfsop_search,gfsens_search,ecmwfens_search = False,False,False
iteration = 0
myCondition = True
model = ""
freezer = freeze_time("2022-06-13 00:00:01")
tz = timezone('US/Eastern')
freezer.start()
while myCondition == True:
    now = datetime.datetime.now(tz)
    target_day = now.date()
   
    curr_time = now
    if iteration % 48 == 0:    
        print(f"Iteration: {iteration}")
    if prev_day != now.day:
        print(f"Day is now {now.date()}, rechecking for forecasts")
        gfsop_hit,gfsens_hit,ecmwfens_hit = False,False,False
    prev_day = now.day
    #check for GFS_Op

    if ((curr_time.hour >= 1 and curr_time.minute >= 15) or curr_time.hour > 1) and not gfsop_hit:
# =============================================================================
#         if iteration % 5 == 0:
#             print("Checking GFS OP")
# =============================================================================
        test_alert.reset_server()
        under_five,under_two,under_zero,today_df = test_alert.check_data(data_source = "GFS",target_date=target_day,run = "Op")

        if len(today_df) != 0:
            curr_model = "GFS_Op"
            if len(under_zero) != 0:
                subject_content,message_content = test_alert.construct_message(under_zero,curr_model,"0")
                test_alert.reset_mail_connection()
                test_alert.send_mail(to = Recipient, sender = Sender, subject = subject_content, text_content = message_content)
            if len(under_two) != 0:
                subject_content,message_content = test_alert.construct_message(under_two,curr_model,"2")
                test_alert.reset_mail_connection()
                test_alert.send_mail(to = Recipient, sender = Sender, subject = subject_content, text_content = message_content)
            if len(under_five) != 0:
                subject_content,message_content = test_alert.construct_message(under_five,curr_model,"5")
                test_alert.reset_mail_connection()
                test_alert.send_mail(to = under_five_recipient, sender = Sender, subject = subject_content, text_content = message_content)
            gfsop_hit = True
            print(f"GFS Op Has been pulled from FTP: {now.date()}")
    #check for GFS_Ens

    if ((curr_time.hour >= 1 and curr_time.minute >= 45) or curr_time.hour > 1)  and not gfsens_hit:
# =============================================================================
#         if iteration % 5 == 0:
#             print("Checking GFS Ens")
# =============================================================================
        test_alert.reset_server()
        under_five,under_two,under_zero,today_df = test_alert.check_data(data_source = "GFS",target_date=target_day,run = "Ens")

        if len(today_df) != 0:
            curr_model = "GFS_Ens"
            if len(under_zero) != 0:
                subject_content,message_content = test_alert.construct_message(under_zero,curr_model,"0")
                test_alert.reset_mail_connection()
                test_alert.send_mail(to = Recipient, sender = Sender, subject = subject_content, text_content = message_content)
            if len(under_two) != 0:
                subject_content,message_content = test_alert.construct_message(under_two,curr_model,"2")
                test_alert.reset_mail_connection()
                test_alert.send_mail(to = Recipient, sender = Sender, subject = subject_content, text_content = message_content) 
            if len(under_five) != 0:
                subject_content,message_content = test_alert.construct_message(under_five,curr_model,"5")
                test_alert.reset_mail_connection()
                test_alert.send_mail(to = under_five_recipient, sender = Sender, subject = subject_content, text_content = message_content)
            gfsens_hit = True
            print(f"GFS Ens Has been pulled from FTP: {now.date()}")
    #check for ECMWF_Ens

    if ((curr_time.hour >= 3 and curr_time.minute >= 50) or curr_time.hour > 3) and not ecmwfens_hit:
# =============================================================================
#         if iteration % 5 == 0:
#             print("Checking ECMWF Ens")
# =============================================================================
        test_alert.reset_server()
        under_five,under_two,under_zero,today_df = test_alert.check_data(data_source = "ECMWF",target_date=target_day,run = "Ens")
        
        if len(today_df) != 0:
            curr_model = "ECMWF_Ens"
            if len(under_zero) != 0:
                subject_content,message_content = test_alert.construct_message(under_zero,curr_model,"0")
                test_alert.reset_mail_connection()
                test_alert.send_mail(to = Recipient, sender = Sender, subject = subject_content, text_content = message_content)
            if len(under_two) != 0:
                subject_content,message_content = test_alert.construct_message(under_two,curr_model,"2")
                test_alert.reset_mail_connection()
                test_alert.send_mail(to = Recipient, sender = Sender, subject = subject_content, text_content = message_content)
            if len(under_five) != 0:
                subject_content,message_content = test_alert.construct_message(under_five,curr_model,"5")
                test_alert.reset_mail_connection()
                test_alert.send_mail(to = under_five_recipient, sender = Sender, subject = subject_content, text_content = message_content)
            ecmwfens_hit = True
            print(f"ECMWF Ens Has been pulled from FTP: {now.date()}")
    iteration += 1 
    
    time.sleep(300) # Sleeps for 5 min  
    
    