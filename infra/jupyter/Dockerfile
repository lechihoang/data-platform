FROM apache/spark:4.0.0

# Switch to root to install packages globally
USER root

# Install dependencies using standard pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Expose port and set default command
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]

# Switch back to spark user
USER 185
