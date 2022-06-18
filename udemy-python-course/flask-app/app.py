from flask import Flask, render_template


app = Flask(__name__)
posts = {
    0: {
        'title': 'Hello, world',
        'content': 'This is my first post!'
    }
}


@app.route('/')
def home():
    return 'Hello World'


@app.route('/post/<int:post_id>')
def post(post_id):
    return render_template('post.html', post=posts.get(post_id))


if __name__ == '__main__':
    app.run(debug=True, port=5001)