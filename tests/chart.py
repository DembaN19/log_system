import pandas as pd
import plotly as px
# Création de DataFrame
df_base = pd.DataFrame({
    "date": ['2021-01-01', '2021-01-02', '2021-01-03'],
    "levelname": ['INFO', 'ERROR', 'INFO'],
    "message": ['Process started', 'Error occurred', 'Process completed'],
    "project_name": ['Project A', 'Project B', 'Project A'],
    "status": ['Success', 'Failure', 'Failure'],
    "duration": ['0:00:30', '0:01:00', '0:00:45'],
    "load_file": ['2021-01-01 12:00', '2021-01-02 12:00', '2021-01-03 12:00']
})

def generate_bar_chart_count(df, col_x_i, col_x_j, col_y, title, bar_type, head=None, col_to_sort=None):
    """
    This function returns a bar chart or line chart with counts of occurrences grouped by two columns.
    The chart includes percentage annotations inside the bars.
    
    Parameters:
    - df: DataFrame with data
    - col_x_i: Column name for x-axis (e.g., project name)
    - col_x_j: Column name for color grouping (e.g., status)
    - col_y: Column to count occurrences (e.g., any column to aggregate)
    - title: Title of the chart
    - bar_type: Type of chart ('bar' or 'line')
    - head: Number of top rows to display (optional)
    - col_to_sort: Column to sort by (optional, otherwise sort by col_y)
    
    Returns:
    - A Plotly figure object
    """
    
    # Group by the specified columns and count occurrences
    if col_to_sort:
        fig_by = df.groupby([col_x_i, col_x_j, col_to_sort])[col_y].count().reset_index(name='count')
    else:
        fig_by = df.groupby([col_x_i, col_x_j])[col_y].count().reset_index(name='count')
    
    # Calculate total counts for each x_i (e.g., project_name)
    totals = fig_by.groupby(col_x_i)['count'].sum().reset_index(name='total')
    
    # Merge totals with the original DataFrame
    fig_by = fig_by.merge(totals, on=col_x_i)
    
    # Calculate percentage
    fig_by['percentage'] = (fig_by['count'] / fig_by['total']) * 100
    
    # Sort the DataFrame based on col_to_sort or count
    if col_to_sort:
        fig_by = fig_by.sort_values(by=col_to_sort)
    else:
        fig_by = fig_by.sort_values(by='count', ascending=False)
    
    # Apply head if specified
    if head is not None:
        fig_by = fig_by.head(head)
    
    # Create the appropriate chart type
    if bar_type == 'bar':
        fig = px.bar(fig_by, x=col_x_i, y='count', color=col_x_j, title=title,
                     text='percentage', labels={'count': 'Count', 'percentage': 'Percentage'})
        fig.update_traces(texttemplate='%{text:.1f}%', textposition='inside')
    elif bar_type == 'line':
        fig = px.line(fig_by, x=col_x_i, y='count', color=col_x_j, title=title, markers=True)
    else:
        raise ValueError(f"Unsupported bar_type: {bar_type}. Use 'bar' or 'line'.")
    
    return fig

# Utilisation de la fonction
fig = generate_bar_chart_count(df_base, 'project_name', 'status', 'message', 
                               'Proportion de Succès et d\'Échecs par Projet', 
                               'bar', head=None)

# Affichage du graphique
fig.show()