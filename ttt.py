from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)


@app.route('/second_route', methods=['POST'])
def second_route():
    print (request.json)
    return redirect(url_for('hello_world', message='test123'))


@app.route('/')
def hello_world(message=None):
    return render_template('index.html', message=message)


app.run()