import os, shutil, json
from flask import Flask, request, jsonify, render_template
from rdbms.executor import Executor

app = Flask(__name__)
executor = Executor()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/run", methods=["POST"])
def run():
    cmd = request.json.get("command", "").strip()
    try:
        if cmd.upper() == "RESET DATABASE":
            if os.path.exists("data"):
                shutil.rmtree("data")
            os.makedirs("data")
            executor.tables.clear()
            return jsonify({"success": True, "result": "Database reset successfully."})
        
        result = executor.execute(cmd)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        return jsonify({"success": False, "result": str(e)})
if __name__ == "__main__":
    app.run(debug=True)
