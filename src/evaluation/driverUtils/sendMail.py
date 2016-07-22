import smtplib

def sendMail(msg):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login("dbgroupdrexel@gmail.com", "ilovedbilovedb")
    server.sendmail("dbgroupdrexel@gmail.com", "vera.zaychik@gmail.com", msg)
    server.quit()