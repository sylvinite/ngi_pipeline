
import smtplib
from email.mime.text import MIMEText




def mail(recipient, subject, text, origin="ngi_pipeline"):
     msg=MIMEText(text)
     msg['Subject'] = '[NGI_pipeline] {}'.format(subject)
     msg['From'] = origin
     msg['To'] = recipient
     s = smtplib.SMTP('localhost', 25)
     s.sendmail('funk_002@nestor1.uppmax.uu.se', recipient, msg.as_string()) 
     s.quit()


def mail_sample_analysis(project_name, sample_name, workflow_name,
        recipient="ngi_pipeline_operators@scilifelab.se",
        subject="sample_analysis intervention needed", origin="ngi_pipeline", exception_text=None):

    text="""
    Project {} / Sample {} / Workflow {}

    This sample analysis has encountered an error.
    Not launching any new analysis.
    
    """.format(project_name, sample_name, workflow_name)
    if exception_text:
        text = text + "{}\n".format(exception_text)
    mail(recipient=recipient, subject=subject, text=text, origin=origin)
