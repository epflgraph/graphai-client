# graphai-client
A client library to access the graphai services


Setup
=====
Installation
------------
You can install the latest version on PyPI using pip:
```bash
pip install graphai-client
```
Alternatively, you could install it (editable or otherwise) from the git repo:

Standard:
```bash
pip install git+https://github.com/epflgraph/graphai-client.git
```
Editable (with pip version >= 21.3):
```bash
pip install -e git+https://github.com/epflgraph/graphai-client.git
```
Authentication
--------------
You then need to prepare a JSON file with your login and password (ask Ramtin if you don't have one yet).
Here is a template for that JSON file, assuming you want to connect to the main GraphAI server (change host/port if you are running it locally): 
```json
{
  "host": "https://graphai.epfl.ch",
  "port": 443,
  "user": "PUT_YOUR_USERNAME_HERE",
  "password": "PUT_YOUR_PASSWORD_HERE"
}
```
The path to this JSON file can be passed as the `graph_api_json` argument for the integrated functions:
```python
from graphai_client.client import process_video

url= 'http://api.cast.switch.ch/p/113/sp/11300/serveFlavor/entryId/0_00gdquzv/v/2/ev/3/flavorId/0_i0v49s5y/forceproxy/true/name/a.mp4'
video_info = process_video(url, graph_api_json='config/graphai-api.json')
```
For the basic API functionalities you first need to log in and then use the resulting login_info as parameter:

```python

from graphai_client.client_api.utils import login
from graphai_client.client_api.translation import translate_text

login_info = login(graph_api_json='config/graphai-api.json')
translated_text = translate_text(text='example', source_language='en', target_language='fr', login_info=login_info)
```

Direct API functionalities
=========================

Functions to access most of the API functionalities are available in `graphai_client.client_api`.
It includes:
- for text:
    - language detection `graphai_client.client_api.translation.detect_language()`
    - translation `graphai_client.client_api.translation.translate_text()`
    - concept extraction `graphai_client.client_api.text.extract_concepts_from_text()`
    - keyword detection `graphai_client.client_api.text.extract_concepts_from_keywords()`
    - embeding `graphai_client.client_api.embedding.embed_text()`
- for videos:
    - video downloading `graphai_client.client_api.video.get_video_token()`
    - video fingerprinting `graphai_client.client_api.video.fingerprint_video()`
    - audio extraction `graphai_client.client_api.video.extract_audio()`
    - slide extraction `graphai_client.client_api.video.extract_slides()`
    - download of resources `graphai_client.client_api.video.download_file()`
- for audio extracted from videos:
    - transcription `graphai_client.client_api.voice.transcribe_audio()`
    - language detection `graphai_client.client_api.voice.detect_language()`
- OCR for slides `graphai_client.client_api.image.extract_text_from_slide()`
    

Integrated video processing
===========================

A function to directly process videos are available as `graphai_client.client.process_video()`:
Below are the steps it implements:
- downloads the video
- if `detect_audio_language` or `analyze_audio` is True, it extracts audio
- if `analyze_audio` is True it transcribes the audio and then translate the transcription 
  or if `detect_audio_language` is True the language of the audio is detected.
- if `analyze_slides` is True, slides are extracted from the video, OCR is performed on them, and the text from the 
  slides is translated.
  

example usage:
```python
from graphai_client.client import process_video


url= 'http://api.cast.switch.ch/p/113/sp/11300/serveFlavor/entryId/0_00gdquzv/v/2/ev/3/flavorId/0_i0v49s5y/forceproxy/true/name/a.mp4'
video_info = process_video(url)
print(video_info)
```
output:
```
[2023-10-19 14:27] [GRAPHAI] [DOWNLOAD VIDEO] [PROCESSING] processing the video http://api.cast.switch.ch/p/113/sp/11300/serveFlavor/entryId/0_00gdquzv/v/2/ev/3/flavorId/0_i0v49s5y/forceproxy/true/name/a.mp4
[2023-10-19 14:27] [GRAPHAI] [DOWNLOAD VIDEO] [WARNING] http://api.cast.switch.ch/p/113/sp/11300/serveFlavor/entryId/0_00gdquzv/v/2/ev/3/flavorId/0_i0v49s5y/forceproxy/true/name/a.mp4 has already been retrieved in the past
[2023-10-19 14:27] [GRAPHAI] [DOWNLOAD VIDEO] [SUCCESS] http://api.cast.switch.ch/p/113/sp/11300/serveFlavor/entryId/0_00gdquzv/v/2/ev/3/flavorId/0_i0v49s5y/forceproxy/true/name/a.mp4 has been retrieved
[2023-10-19 14:27] [GRAPHAI] [EXTRACT SLIDES] [PROCESSING] extracting slides
[2023-10-19 14:27] [GRAPHAI] [EXTRACT SLIDES] [SUCCESS] 1 slides has been extracted from 169770835520421902463099.mp4
[2023-10-19 14:27] [GRAPHAI] [EXTRACT TEXT FROM SLIDES] [PROCESSING] extracting text from 1 slides
[2023-10-19 14:27] [GRAPHAI] [OCR] [SLIDE 1/1] [SUCCESS] text has been extracted from 169770835520421902463099.mp4_slides/frame-000041.png
[2023-10-19 14:27] [GRAPHAI] [TRANSLATE] [PROCESSING] translate text from 1 slides
[2023-10-19 14:27] [GRAPHAI] [TRANSLATE] [SLIDE 1/1] [SUCCESS] text has been translated
[2023-10-19 14:27] [GRAPHAI] [EXTRACT AUDIO] [PROCESSING] extracting audio
[2023-10-19 14:27] [GRAPHAI] [EXTRACT AUDIO] [SUCCESS] audio has been extracted from 169770835520421902463099.mp4
[2023-10-19 14:27] [GRAPHAI] [TRANSCRIBE] [PROCESSING] transcribe audio
[2023-10-19 14:27] [GRAPHAI] [TRANSCRIBE] [SUCCESS] 5 segments have been extracted from 169770835520421902463099.mp4_audio.ogg
[2023-10-19 14:27] [GRAPHAI] [TRANSLATE] [PROCESSING] translate transcription for 5 segments
[2023-10-19 14:27] [GRAPHAI] [TRANSLATE] [SUBTITLE 1/5] [SUCCESS] text has been translated
[2023-10-19 14:27] [GRAPHAI] [TRANSLATE] [SUBTITLE 2/5] [SUCCESS] text has been translated
[2023-10-19 14:27] [GRAPHAI] [TRANSLATE] [SUBTITLE 3/5] [SUCCESS] text has been translated
[2023-10-19 14:27] [GRAPHAI] [TRANSLATE] [SUBTITLE 4/5] [SUCCESS] text has been translated
[2023-10-19 14:27] [GRAPHAI] [TRANSLATE] [SUBTITLE 5/5] [SUCCESS] text has been translated
[2023-10-19 14:27] [GRAPHAI] [VIDEO] [SUCCESS] The video http://api.cast.switch.ch/p/113/sp/11300/serveFlavor/entryId/0_00gdquzv/v/2/ev/3/flavorId/0_i0v49s5y/forceproxy/true/name/a.mp4 has been successfully processed
{'url': 'http://api.cast.switch.ch/p/113/sp/11300/serveFlavor/entryId/0_00gdquzv/v/2/ev/3/flavorId/0_i0v49s5y/forceproxy/true/name/a.mp4', 'video_token': '169770835520421902463099.mp4', 'slides': [{'token': '169770835520421902463099.mp4_slides/frame-000041.png', 'timestamp': 41, 'en': 'EPFL Bacteria GFP expression\nquantification\n• IP4LS-2022-Projects\n3 conditions,\n5\n2 ch / images\nimages/conditions\n11\nAme Seitz-Romain Guiet-Nicolas Chiaruttini-Olivier Burri', 'fr': " Expression GFP Bactéria de l'EPFL. quantification. • IP4LS-2022-Projets. 3 conditions, 5. 2 ml / images. images/conditions. 11. Ame Seitz-Romain Guiet-Nicolas Chiaruttini-Olivier Burri"}], 'slides_language': 'en', 'subtitles': [{'start': 0.0, 'end': 5.0, 'en': 'Bacteria GFP Expression', 'fr': 'Bacteria GFP Expression'}, {'start': 5.0, 'end': 17.0, 'en': 'The data consists of bacteria images acquired in face contrast and fluorescence across three different conditions, A, B, and C, with five replicates per condition.', 'fr': "Les données se composent d'images de bactéries acquises dans le contraste du visage et la fluorescence dans trois conditions différentes, A, B et C, avec cinq répliques par condition."}, {'start': 17.0, 'end': 24.0, 'en': "The fluorescent channel represents a GFP-tagged protein expressed in the bacteria's protoplasm.", 'fr': 'Le canal fluorescent représente une protéine marquée GFP exprimée dans le protoplasme de la bactérie.'}, {'start': 25.0, 'end': 31.0, 'en': 'Because the bacteria look quite separated, the experimenter would like to obtain per-bacteria measurements.', 'fr': "Comme les bactéries ont l'air assez séparées, l'expérimentateur aimerait obtenir des mesures par bactérie."}, {'start': 31.0, 'end': 40.0, 'en': 'The experimenter is interested in fighting out if there is a significant change in the expression level of his fluorescent protein across the three conditions.', 'fr': "L'expérimentateur s'intéresse à la lutte contre le changement significatif du niveau d'expression de sa protéine fluorescente dans les trois conditions."}], 'audio_language': 'en'}
```

Processing for integration in RCP cluster
=========================================

Scripts to process videos and send the results to MySQL are available in `graphai_client.rcp`. 
These functions are mainly intended to be used by the docker image to be deployed on the RCP cluster.
To use those, you need an additional JSON file containing the login and password for MySQL, here is the template: 
```json
{
  "host": "localhost",
  "port": 3306,
  "user": "PUT_YOUR_USERNAME_HERE",
  "password": "PUT_YOUR_PASWORD_HERE"
}
```
The path to this JSON file can be passed as the `piper_mysql_json_file` argument:
```python
from graphai_client.rcp.get_video_info import  get_video_info_on_rcp

get_video_info_on_rcp(
    ['http://api.cast.switch.ch/p/113/sp/11300/serveFlavor/entryId/0_00gdquzv/v/2/ev/3/flavorId/0_i0v49s5y/forceproxy/true/name/a.mp4'], 
    graph_api_json='config/graphai-api.json', piper_mysql_json_file='config/piper_db.json'
)
```

## License

This project is licensed under the [Apache License 2.0](./LICENSE).