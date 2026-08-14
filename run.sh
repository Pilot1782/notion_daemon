git fetch origin/main
git reset --hard origin/main
git pull

python3 -m pip install -r ./requirements.txt
python3 -m playwight install
python3 ./main.py
