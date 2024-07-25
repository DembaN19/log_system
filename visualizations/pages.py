# Here we will define all functions that hold visuals that will be used into our project

import streamlit as st
from src.utils import get_data, config_file, generate_report, send_email_with_attachment
from pyhocon import ConfigFactory
import plotly.express as px
from fpdf import FPDF
import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import tempfile
from visualizations.charts import *
import pandas as pd

config = ConfigFactory.parse_file(config_file)
server = config.db_dwh.server
database = config.db_dwh.database
username = config.db_dwh.username
password = config.db_dwh.password
schema_table = config.db_dwh.schema_table
sql_data = config.sql_path.sql_data


st.set_page_config(page_title='Sales WEB APP', page_icon="📊", layout="wide", initial_sidebar_state="expanded")
# Grab data from our table
@st.cache_data
def getting_data() -> pd.DataFrame:
    df_base = get_data(server, database, username, password, sql_data)
    
    return df_base

df_base = getting_data()

def show_home():

    # Header
    st.title("📊 Digital & Consulting Revenues")

    # Introduction Section
    st.markdown("""
    ## Welcome to the Digital & Consulting Revenues Dashboard! 👋
    
    This dashboard provides an overview of the revenues generated from our digital and consulting services. Here, you'll be able to compare the current period (N) and the year-to-date (YTD) revenues, and see how digital revenues contribute to our total revenues.
    
    ### What You'll Find Here:
    
    - **📈 Current Period (N) Revenues**: Understand the revenue performance for the most recent period.
    - **📊 Year-To-Date (YTD) Revenues**: See the cumulative revenue performance for the year so far.
    - **💡 Digital Revenue Contribution**: Discover the share of digital revenues in our total revenues.
    
    ## Key Metrics
    ### Current Period (N)
    - **Total Revenue (N)**: The total revenue generated in the current period.
    - **Digital Revenue (N)**: The portion of the current period's revenue that comes from digital services.
    - **Digital Percentage (N)**: The percentage of the total revenue that is contributed by digital services.

    ### Year-To-Date (YTD)
    - **Total Revenue (YTD)**: The total revenue generated from the start of the year to the current date.
    - **Digital Revenue (YTD)**: The portion of the YTD revenue that comes from digital services.
    - **Digital Percentage (YTD)**: The percentage of the YTD revenue that is contributed by digital services.

    ## Why This Matters 🚀
    Understanding the breakdown of our revenues helps us:
    - **Identify Growth Opportunities**: Pinpoint areas where digital services are thriving and where there's room for improvement.
    - **Make Data-Driven Decisions**: Use revenue insights to guide our strategic planning and resource allocation.
    - **Enhance Digital Strategies**: Focus on enhancing our digital offerings to boost overall performance.
    
    ## Next Steps 📅
    Explore the dashboard to gain deeper insights into our revenue performance. Use the data to inform your strategies and drive growth. If you have any questions or need further analysis, feel free to reach out to the finance team.
    
    ### Thank you for using the Digital & Consulting Revenues Dashboard! 🙏
    """)

def show_analytics():
    
    # first grap data 
    
    
    
    st.title("Sales Analytics Dashboard")
    st.write("Welcome to the sales analytics dashboard. Here, you can find insights into sales performance across different entities, salespersons, and business dimensions.")

    st.header("Sales Data Overview")
    with st.expander("Data Overview"):
        st.dataframe(df_base)

    left_column, midd_column, right_column = st.columns([1, 1, 1])
    
    current_year: int = datetime.now().year
    previous_year = current_year - 1
   
   
    
    
def show_reports():
    
    st.title("Sales Reports")
    st.write("Generate and download detailed sales reports.")

    st.header("Sales Data Overview")
    with st.expander("Data Overview"):
        st.dataframe(df_base)

    st.header("Download Reports")
    generate_report(df_base)



    # PDF Report
    # Generate the pivot table
    pivot_table = df_base.pivot_table(
        index=['entity', 'ods', 'yeargl'],
        values='amount',
        aggfunc='sum'
    ).reset_index()

    # Calculate the percentage of digital sales regarding the CA
    pivot_table['Percentage'] = (pivot_table['Sales'] / df_base.groupby(['Entity', 'BusinessLine', 'YearGL'])['CA'].first().values) * 100

    pivot_table['Sales'] = pivot_table['Sales'].round(2)
    pivot_table['Percentage'] = pivot_table['Percentage'].round(2)
    # Streamlit button for generating PDF report
    if st.button("Generate PDF Report"):
        
        # Create a plot of the pivot table
        fig, ax = plt.subplots()
        ax.axis('tight')
        ax.axis('off')
        ax.table(cellText=pivot_table.values, colLabels=pivot_table.columns, cellLoc='center', loc='center')

        # Save the plot as a PNG file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmpfile:
            plt.savefig(tmpfile.name, format='png')
            plt.close(fig)
            image_path = tmpfile.name

        # Generate PDF report
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=14)

        pdf.cell(200, 30, txt="DCX Sales Report", ln=True, align='C')
        pdf.ln(20)

        # Add the pivot table image to the PDF
        pdf.image(image_path, x=20, y=40, w=180)  # Adjust the position and size as needed

        pdf_output = pdf.output(dest='S').encode('latin1')

        

        st.download_button(
            label="Download PDF Report",
            data=pdf_output,
            file_name='sales_report.pdf',
            mime='application/pdf',
        )
    
def show_contacts():
    st.title("Contact Us")
    st.write("Please fill out the form below to reach out to us for new requests, report bugs, or provide feedback.")

    # Form
    with st.form(key='contact_form'):
        name = st.text_input("Name")
        role = st.text_input("Role")
        needs = st.text_area("Needs / Feedback")
        email = st.text_input("Email")
        
        submit_button = st.form_submit_button(label='Submit')

    # Form submission handling
    if submit_button:
        if name and role and needs and email:
            st.success("Thank you for your submission! We will get back to you soon.")
            # Here you can handle the form submission, e.g., save the data to a database, send an email, etc.
            # For demonstration, we'll just print the form data
            form_data = {
                "Name": name,
                "Role": role,
                "Needs": needs,
                "Email": email
            }
            form_data_str = '\n'.join([f"{key}: {value}" for key, value in form_data.items()])
            
            send_email_with_attachment(subject="New request for DCX", body=form_data_str, recipient_list=config.email.recipient_list)
        else:
            st.error("Please fill in all the required fields.")
    