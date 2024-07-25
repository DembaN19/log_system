import streamlit as st
import plotly.express as px
import plotly.graph_objs as go
import matplotlib.pyplot as plt
import pandas as pd 


# Here we will define all charts for this project

def generate_simple_bar_chart(df: pd.DataFrame, col_x: str, col_y: str, title: str, bar_type):
    """
    This simple function will return a bar chart with on column group with a sum calculation
    
    """
    
    fig_by = df.groupby(col_x)[col_y].sum().sort_values(ascending=False).reset_index()
    if bar_type == 'bar':
        fig = px.bar(fig_by, x=col_x, y=col_y, title=title)
    elif bar_type == 'line':
        fig = px.line(fig_by, x=col_x, y=col_y, title=title, markers=True)
    return fig


def generate_bar_chart(df, col_x_i, col_x_j, col_y, title, bar_type, head: int=None):
    """
    This function will return a bar chart with two columns grouped with a sum calculation
    
    """
    if head != None:
        fig_by = df.groupby([col_x_i, col_x_j])[col_y].sum().reset_index().sort_values(by=col_y, ascending=False).head(head)
    else:
        fig_by = df.groupby([col_x_i, col_x_j])[col_y].sum().reset_index().sort_values(by=col_y, ascending=False)
        
    if bar_type == 'bar':
        fig = px.bar(fig_by, x=col_x_i, y=col_y, title=title, color=col_x_j)
    elif bar_type == 'line':
        fig = px.line(fig_by, x=col_x_i, y=col_y, title=title, markers=True, color=col_x_j)
    else:
        raise ValueError(f"Unsupported bar_type: {bar_type}. Use 'bar' or 'line'.")
        
    return fig



def generate_pie_chart(df, col_x, col_y, title, colors):
    """
    This simple function will return a pie chart with on column group with a sum calculation
    
    """
    fig_by = df.groupby(col_x)[col_y].sum().reset_index().sort_values(by=col_y, ascending=False)
    fig = px.pie(fig_by, values=col_y, names=col_x, title=title, template="plotly_white", color_discrete_sequence=colors)
    return fig