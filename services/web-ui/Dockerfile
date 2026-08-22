FROM python:3.13-alpine AS data-generator

WORKDIR /build

COPY iso639-2.txt .
COPY fetch_opus_models.py .
COPY generate_language_json.py .

RUN python fetch_opus_models.py
RUN python generate_language_json.py


FROM nginx:1.27-alpine

COPY index.html /usr/share/nginx/html/index.html
COPY style.css /usr/share/nginx/html/style.css

COPY --from=data-generator /build/languages.json /usr/share/nginx/html/languages.json
COPY --from=data-generator /build/language_pairs.json /usr/share/nginx/html/language_pairs.json

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
