from flask import Flask, render_template, request, redirect, jsonify

from api import query_wiki_assistant, get_most_outdated_page, get_page_links_by_position_on_page

app = Flask(__name__)


@app.route("/")
def index():
    return render_template('index.html')


@app.route("/api_1_result/", methods=['POST'])
def api_1():
    res = '123'
    sql_query = request.form['sql_query']
    print("The query is '" + sql_query + "'")

    try:
        result = query_wiki_assistant(sql_query=sql_query, as_dict=True)
    except:
        return 'The SQL query you entered is either invalid or did not work'

    # return render_template('api_1_result.html', **locals())
    return jsonify(result)


@app.route("/api_2_result/", methods=['POST'])
def api_2():
    r = '456'
    category = request.form['category']
    print("The category is '" + category + "'")

    try:
        result = get_most_outdated_page(category=category, as_dict=True)
    except:
        return 'The category you entered is either invalid or did not work'

    return jsonify(result)


@app.route("/api_3_result/", methods=['POST'])
def api_3():
    r = '789'
    page_title = request.form['page_title']
    print("The page_title is '" + page_title + "'")
    try:
        result = get_page_links_by_position_on_page(page_title=page_title, as_dict=True)
    except:
        return 'The page title you entered is either invalid or did not work'

    return jsonify(result)


# @app.route('/<string:page_name>/')
# def render_static(page_name):
#     return render_template('%s.html' % page_name)


if __name__ == "__main__":
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    app.run(host='0.0.0.0', port=8080)
