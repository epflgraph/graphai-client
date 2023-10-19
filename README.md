# graphai-client
A client library to access the graphai services

example usage:
```python
from graphai.client import process_video


url= 'http://api.cast.switch.ch/p/113/sp/11300/serveFlavor/entryId/0_00gdquzv/v/2/ev/3/flavorId/0_i0v49s5y/forceproxy/true/name/a.mp4'
video_info = process_video(url, force=True)
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
