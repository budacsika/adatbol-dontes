FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["functions-framework", "--source=app/main.py", "--target=process_arpadent_zip", "--signature-type=cloudevent", "--host=0.0.0.0", "--port=8080"]