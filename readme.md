to run this project, you need to do following things:

1. go to file elastic_search.py, change the username and password to the one you are using in your local environment.
2. change the param file_dir to your local folder where you put all the doc file. 

   if you want to see the version that removes stop word and is stemmed, do following:
   1. change is_stem to true, 
   2. set param stop_word_dir and stem_word_dir to corresponding directory 
3. go to app.py, run the file, and ggo have a cup of tea. When you see * Running on when you see http://127.0.0.1:5000, click it, and you can see the webpage
4. type the query sentence in the search box, wait for about one second, and you will have your result 