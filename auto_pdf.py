from datetime import datetime
import logging as logger
from src.utils import send_email_with_attachment, generate_pdf_report_from_df_, diff_time, config

def main():
    from visualizations.pages import df_base
    start_timestamp = datetime.now()
    logger.info(f'Start timestamp: {datetime.now()}')
    
    date_of_report = datetime.now().strftime('%Y-%m-%d')
    # date_of_report = '2024-07-05'
    df_base['date_only'] = df_base['date'].dt.date
    df_base['date_only'] = df_base['date_only'].astype(str)
    df_base = df_base.loc[df_base['date_only'] == date_of_report]
    subject = config.email.subject
    subject = f"{subject} - {date_of_report}"
    body = config.email.body
    recipient_list = config.email.recipient_list
    pdf_output_path = f"reports/project_status_report_{date_of_report}.pdf"
    attachment_paths = [pdf_output_path]
    
    if len(df_base) > 0:
        generate_pdf_report_from_df_(df_base, pdf_output_path)

        send_email_with_attachment(subject, body, attachment_paths, recipient_list)
    else:
        logger.error("No data to send !")
    

    # Time duration
    end_timestamp = datetime.now()
    logger.info(f'Traitement finished timestamp: {end_timestamp}')
    traitement_duration = diff_time(start_timestamp , end_timestamp)
    logger.info(f'Duration job: {traitement_duration[0]}h:{traitement_duration[1]}m:{traitement_duration[2]}s ')

if __name__ == "__main__":
    main()
    