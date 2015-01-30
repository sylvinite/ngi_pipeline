
import smtplib
from email.mime.text import MIMEText
import traceback



def mail(recipient, subject, text, origin="ngi_pipeline"):
     msg=MIMEText(text)
     msg['Subject'] = '[NGI_pipeline] {}'.format(subject)
     msg['From'] = origin
     msg['To'] = recipient
     s = smtplib.SMTP('localhost', 25)
     s.sendmail('funk_002@nestor1.uppmax.uu.se', recipient, msg.as_string()) 
     s.quit()


def mail_analysis(project_name, sample_name = None, engine_name = None,
                  recipient="ngi_pipeline_operators@scilifelab.se",
                  subject="analysis intervention needed",
                  origin="ngi_pipeline", info_text=None):
    file_name, line_no = traceback.extract_stack(limit=2)[-2][:2]
    text = "This analysis has encountered an error:"
    text += "\nProject: {}".format(project_name)
    if sample_name:
        text += "\nSample: {}".format(sample_name)
    if engine_name:
        text += "\nEngine: {}".format(engine_name)
    text += "\nFile: {}".format(file_name)
    text += "\nLine: {}".format(line_no)
    if info_text:
        text = text + "\n\nAdditional information about this error:\n{}\n".format(info_text)
    mail(recipient=recipient, subject=subject, text=text, origin=origin)
