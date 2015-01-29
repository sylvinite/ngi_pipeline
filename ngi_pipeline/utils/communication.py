
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
        subject="analysis intervention needed", origin="ngi_pipeline", info_text=None):
    tb_info=traceback.extract_stack(limit=2)[-2]
    text="Project {}".format(project_name)
    if sample_name:
        text+=" / Sample {}".format(sample_name)
    if engine_name:
        text+=" / Engine {}".format(engine_name)

    text+="""
    File : {} / Line : {}

    This analysis has encountered an error.
    Not launching any new analysis.
    
    """.format(tb_info[0],tb_info[1])
    if info_text:
        text = text + "{}\n".format(info_text)
    mail(recipient=recipient, subject=subject, text=text, origin=origin)
