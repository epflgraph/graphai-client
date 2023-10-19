# graphai-client
A client library to access the graphai services

example usage:
```python
from graphai.client import process_video


url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00gdquzv'
video_info = process_video(url, force=True)
print(video_info)
```
output:
```
[2023-10-17 17:37] [GRAPHAI] [DOWNLOAD VIDEO] [PROCESSING] processing the video https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00gdquzv/format/download/protocol/https/flavorParamIds/0
[2023-10-17 17:37] [GRAPHAI] [DOWNLOAD VIDEO] [WARNING] https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00gdquzv/format/download/protocol/https/flavorParamIds/0 has already been retrieved in the past
[2023-10-17 17:37] [GRAPHAI] [EXTRACT SLIDES] [PROCESSING] extracting slides
[2023-10-17 17:37] [GRAPHAI] [EXTRACT TEXT FROM SLIDES] [PROCESSING] extracting text from 1 slides
[2023-10-17 17:38] [GRAPHAI] [OCR] [SLIDE 1/1] [WARNING] document text detection result not found
[2023-10-17 17:38] [GRAPHAI] [TRANSLATE] [PROCESSING] translate text from 1 slides
[2023-10-17 17:38] [GRAPHAI] [EXTRACT AUDIO] [PROCESSING] extracting audio
[2023-10-17 17:38] [GRAPHAI] [TRANSCRIBE] [PROCESSING] transcribe audio
[2023-10-17 17:38] [GRAPHAI] [TRANSLATE] [PROCESSING] translate transcription for 5 segments
[2023-10-17 17:39] [GRAPHAI] [VIDEO] [SUCCESS] The video https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00gdquzv/format/download/protocol/https/flavorParamIds/0 has been successfully processed
{'url': 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00gdquzv/format/download/protocol/https/flavorParamIds/0', 'video_token': '169262043965565107115351.mp4', 'slides': [{'token': '169262043965565107115351.mp4_slides/frame-000041.png', 'timestamp': 41, 'en': '▪ IP4LS - 2022 - Projects\n5\n2 ch / images\nimages/conditions\n3 conditions,\n2\nEPFL\nquantification\nBacteria GFP expression\nArne Seitz - Romain Guiet - Nicolas Chiaruttini - Olivier Burri\n11', 'fr': ' ▪ IP4LS - 2022 - Projets. 5. 2 ml / images. images/conditions. 3 conditions, 2. EPFL. quantification. Bacteria GFP expression. Arne Seitz - Romain Guiet - Nicolas Chiaruttini - Olivier Burri. 11'}], 'slides_language': 'en', 'subtitles': [{'start': 0.0, 'end': 5.0, 'en': 'Bacteria GFP expression.', 'fr': 'Bacteria GFP expression.'}, {'start': 5.0, 'end': 17.0, 'en': 'The data consists of bacteria images acquired in face contrast and fluorescence across three different conditions, A, B, and C, with five replicates per condition.', 'fr': "Les données se composent d'images de bactéries acquises dans le contraste du visage et la fluorescence dans trois conditions différentes, A, B et C, avec cinq répliques par condition."}, {'start': 17.0, 'end': 24.0, 'en': "The fluorescent channel represents a GFP-tagged protein expressed in the bacteria's protoplasm.", 'fr': 'Le canal fluorescent représente une protéine marquée GFP exprimée dans le protoplasme de la bactérie.'}, {'start': 24.0, 'end': 31.0, 'en': 'Because the bacteria look quite separated, the experimenter would like to obtain per-bacteria measurements.', 'fr': "Comme les bactéries ont l'air assez séparées, l'expérimentateur aimerait obtenir des mesures par bactérie."}, {'start': 31.0, 'end': 40.0, 'en': 'The experimenter is interested in fighting out if there is a significant change in the expression level of his fluorescent protein across the three conditions.', 'fr': "L'expérimentateur s'intéresse à la lutte contre le changement significatif du niveau d'expression de sa protéine fluorescente dans les trois conditions."}], 'audio_language': 'en'}
```