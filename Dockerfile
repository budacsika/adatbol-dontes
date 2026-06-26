FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["functions-framework", "--target=process_arpadent_zip", "--signature-type=cloudevent", "--port=8080"]