import smtplib

def sendMail(email, msg):
    server = smtplib.SMTP_SSL('smtp.googlemail.com', 465)
    #server.starttls()
    server.login("dbgroupdrexel@gmail.com", "ilovedbilovedb")
    server.sendmail("dbgroupdrexel@gmail.com", email, msg)
    server.quit()
