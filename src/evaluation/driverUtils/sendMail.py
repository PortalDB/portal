import smtplib

def sendMail(email, msg):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login("dbgroupdrexel@gmail.com", "ilovedbilovedb")
    server.sendmail("dbgroupdrexel@gmail.com", email, msg)
    server.quit()