import smtplib
from email.message import EmailMessage

CIRTAP_MAILER = "cirtap@mydomain.com"


def send_start_mail(
    recipients,
    db_dir,
    no_of_genomes,
):
    msg = EmailMessage()

    msg["Subject"] = "[CIRTAP-BOT] PATRIC update"
    msg["From"] = CIRTAP_MAILER
    msg["To"] = recipients

    start_content = (
        "A mirror job for PATRIC was launched by cirtap.\n"
        "Details:\n"
        "\n"
        "Local dir: {}\n"
        "No. of genomes: {}\n"
        "\n"
        "You may receive another email once the run finishes successfully or "
        "fails\n"
    ).format(db_dir.resolve(), no_of_genomes)

    msg.set_content(start_content)

    s = smtplib.SMTP("localhost")
    s.sendmail(CIRTAP_MAILER, recipients, msg.as_string())
    s.quit()


def send_exit_mail(recipients, error_msg=None):
    msg = EmailMessage()

    msg["Subject"] = "[CIRTAP-BOT] PATRIC update"
    msg["From"] = CIRTAP_MAILER
    msg["To"] = recipients

    if error_msg:
        msg_pre = "Cirtap failed!"
        error_content = "Error was : {}".format(error_msg)

    else:
        msg_pre = "Cirtap succeeded!"
        error_content = ""

    message_txt = "\n".join([msg_pre, error_content])
    msg.set_content(message_txt)
    s = smtplib.SMTP("localhost")
    s.sendmail(CIRTAP_MAILER, recipients, msg.as_string())
    s.quit()
