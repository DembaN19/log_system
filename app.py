import streamlit as st
from streamlit_navigation_bar import st_navbar
from visualizations.pages import *
from pyhocon import ConfigFactory
from src.utils import config_file
import warnings
st.set_page_config(page_title='Logging System WEB APP', page_icon="📊", layout="wide", initial_sidebar_state="expanded")
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')


def main():
    
    config = ConfigFactory.parse_file(config_file)
    pages = config.streamlit.pages
    styles = config.streamlit.styles
    options = config.streamlit.options
    linkedin_url = config.streamlit.urls
    logo_logging_system = config.streamlit.logo_logging
    
   
    page = st_navbar(
        pages,
        logo_path=logo_logging_system,
        urls=linkedin_url,
        styles=styles,
        options=options,
    ).lower() 
    
    functions = {
    "home": show_home,
    "analytics": show_analytics,
    "reports": show_reports,
    "contacts": show_contacts
    }
    
    go_to = functions.get(page)
    if go_to:
        go_to()
    else:
        st.error(f"Page '{page}' not found in the configuration.")
    
    
def authenticate():
    config = ConfigFactory.parse_file(config_file)
    # Initialize session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        password = st.text_input("Enter the password: ", type="password", placeholder="Enter the password:")
        if st.button("Validate"):
            if password == config.streamlit.token_pass:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("The password for this page is not correct, contact the support-info team.")
    else:
        main()

if __name__ == '__main__':
    main()
