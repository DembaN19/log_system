import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

def generate_pdf_report_from_df(df, output_path):
    # Create a pivot table from the DataFrame
    pivot_table = df.pivot_table(
        index=['project_name', 'status', 'message'],
        values='duration',
        aggfunc='sum'
    ).reset_index()

    # Create a plot of the pivot table
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis('tight')
    ax.axis('off')
    
    # Create the table
    table = ax.table(cellText=pivot_table.values, colLabels=pivot_table.columns, cellLoc='center', loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.2)
    
    # Adjust column widths
    column_widths = [1, 1, 3, 1]  # 'message' column width is 3 times others
    for i, width in enumerate(column_widths):
        table.auto_set_column_width([i])
        for j in range(len(pivot_table) + 1):  # +1 for header row
            table[(j, i)].set_width(width * 0.2)  # Adjust scale factor as needed

    # Save the plot to a PDF
    with PdfPages(output_path) as pdf:
        pdf.savefig(fig, bbox_inches='tight')
    
    plt.close(fig)

# Example usage
data = {
    'project_name': ['Project A', 'Project B', 'Project A', 'Project B'],
    'status': ['Completed', 'In Progress', 'In Progress', 'Completed'],
    'message': ['This is a short message', 'This message is a bit longer', 'Short msg', 'A very very long message that needs more space'],
    'duration': [120, 300, 150, 400]
}

df = pd.DataFrame(data)
generate_pdf_report_from_df(df, 'output.pdf')