from AlertSystem import alert_system
import pandas as pd
from datetime import datetime
from freezegun import freeze_time
import time
#credentials = pd.read_excel("credentials.xlsx")
#Initialize Alert System
test_alert = alert_system.Alert("","","")
test_alert.set_mail_connection("", "")

Recipient = ""
Sender = ""

curr_hour = None
prev_day = None
gfsop_hit,gfsens_hit,ecmwfens_hit = False,False,False
gfsop_search,gfsens_search,ecmwfens_search = False,False,False
iteration = 0

freezer = freeze_time("2022-05-14 00:00:01")
freezer.start()
now = datetime.datetime.now()
myCondition = True
model = ""
while myCondition == True:
    target_day = now.date()
    curr_time = now
    print(f"Iteration: {iteration}")
    if prev_day != now.day:
        print(f"Day is now {str(now)}, rechecking for forecasts")
        gfsop_hit,gfsens_hit,ecmwfens_hit = False,False,False
    else:
        print("Will not recheck")
    #check for GFS_Op
    print(f"gfsop_hit is {gfsop_hit}")
    if ((curr_time.hour >= 1 and curr_time.minute >= 15) or curr_time.hour > 1) and not gfsop_hit:
        print("Checking GFS OP")
        test_alert.reset_server()
        under_five,under_two,under_zero,today_df = test_alert.check_data(data_source = "GFS",target_date=target_day,run = "Op")
        print(f"LENGTH OF GFS OP IS {len(under_five)}")
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
                test_alert.send_mail(to = Recipient, sender = Sender, subject = subject_content, text_content = message_content)
            gfsop_hit = True
    #check for GFS_Ens
    print(f"gfsens_hit is {gfsens_hit}")
    if ((curr_time.hour >= 1 and curr_time.minute >= 45) or curr_time.hour > 1)  and not gfsens_hit:
        print("Checking GFS Ens")
        test_alert.reset_server()
        under_five,under_two,under_zero,today_df = test_alert.check_data(data_source = "GFS",target_date=target_day,run = "Ens")
        print(f"LENGTH OF GFS Ens IS {len(under_five)}")
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
                test_alert.send_mail(to = Recipient, sender = Sender, subject = subject_content, text_content = message_content)
            gfsens_hit = True
    #check for ECMWF_Ens
    print(f"ecmwfens_hit is {ecmwfens_hit}")
    if ((curr_time.hour >= 3 and curr_time.minute >= 50) or curr_time.hour > 3) and not ecmwfens_hit:
        print("Checking ECMWF Ens")
        test_alert.reset_server()
        under_five,under_two,under_zero,today_df = test_alert.check_data(data_source = "ECMWF",target_date=target_day,run = "Ens")
        print(f"LENGTH OF ECMWF Ens IS {len(under_five)}")
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
                test_alert.send_mail(to = Recipient, sender = Sender, subject = subject_content, text_content = message_content)
            ecmwfens_hit = True
    iteration += 1 
    prev_day = now.day
    now += datetime.timedelta(hours = 1)
    time.sleep(5) # Sleeps for 5 min
    
freezer.stop()
    