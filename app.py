from flask import Flask, render_template, request
from elastic_search import build, search

app = Flask(__name__)
index_name = "test"
app.config['stem_global'], app.config['stop_global'] = build(index_name)


@app.route('/')
def test():  # put application's code here
    # result = ["test","test","test"]
    return render_template("page.html")


@app.route('/query', methods=['POST'])
def query():  # put application's code here
    query = request.form['query']
    stem_global = app.config['stem_global']
    stop_global = app.config['stop_global']
    result = search(query, stem_global, stop_global, size=10)
    return render_template("page.html", data=result)


if __name__ == '__main__':
    app.run()
