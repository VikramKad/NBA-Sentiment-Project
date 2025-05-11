
def read_player_list(file_path='players.txt'):
    with open(file_path, 'r') as f:
        players = [line.strip() for line in f if line.strip()]
    return players