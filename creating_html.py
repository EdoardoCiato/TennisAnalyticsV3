from jinja2 import Template

def generate_report(data):
    with open("index_chat.html") as f:
        template = Template(f.read())

    return template.render(data)

data = {
    "players": {
        "player1": "Lorenzo Sonego",
        "player2": "Mariano Navone"
    },

    "serve": [
        {"label": "Indice Servizio", "p1": 58.1, "p2": 24.1, "delta": "+34.0"},
        {"label": "Efficienza", "p1": 56.8, "p2": 43.6, "delta": "+13.2"}
    ],
}
html = generate_report(data)
print(html)
with open ('index2.html', 'w') as f:
    f.write(html)