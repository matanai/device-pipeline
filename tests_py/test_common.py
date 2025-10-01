from lambdas.common import json_response, html_response

def test_json_response():
    r = json_response(400, {"error": "nope"})
    assert r["statusCode"] == 400
    assert r["headers"]["Content-Type"] == "application/json"
    assert r["body"] == '{"error": "nope"}'

def test_html_response():
    r = html_response(200, "<p>ok</p>")
    assert r["statusCode"] == 200
    assert r["headers"]["Content-Type"] == "text/html"
    assert r["body"] == "<p>ok</p>"