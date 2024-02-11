# web_scraper

## creating local postgres database
### requirements:
    installed Docker

### command for creating postgres database
```shell
docker run -d  -p 5432:5432 -e POSTGRES_USER=ci -e POSTGRES_PASSWORD=SomeRandomPassword! -e POSTGRES_DB=football postgres
```

## Run python script
```shell
python playersScraper.py playerURLs.csv
```

## Description
Retrieving data from wikipedia was taken from infobox.
By studying the data, footballers have something in the 'Senior carrer*' or 'International career‡' category, I don't consider urls that don't have that part on the page as footballers.

Under the national team, we take data from 'International career‡', but it is only considered if the footballer is currently playing in the national team.

Players who have a 'current team' could play in that club for an intermittent period. When we search for appearances and goals, we summarize by all periods when the player played
