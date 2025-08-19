import sqlite3
import hashlib

# --- FUNÇÕES DE HASH (AS MESMAS DO LOGIN) ---

def make_hashes(password):
    """Cria um hash SHA256 para uma senha."""
    return hashlib.sha256(str.encode(password)).hexdigest()

# --- FUNÇÕES DO BANCO DE DADOS ---

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados SQLite."""
    conn = sqlite3.connect('users.db')
    conn.row_factory = sqlite3.Row # Permite acessar colunas por nome
    return conn

def create_usertable():
    """Cria a tabela de usuários se ela não existir."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS userstable(
            name TEXT,
            email TEXT PRIMARY KEY,
            password TEXT,
            role TEXT
        )
    ''')
    conn.commit()
    conn.close()

def add_user(name, email, password, role):
    """Adiciona um novo usuário ao banco de dados."""
    conn = get_db_connection()
    c = conn.cursor()
    hashed_password = make_hashes(password)
    c.execute('INSERT INTO userstable(name, email, password, role) VALUES (?, ?, ?, ?)', (name, email, hashed_password, role))
    conn.commit()
    conn.close()

def get_user(email):
    """Busca um usuário pelo email."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM userstable WHERE email = ?', (email,))
    user_data = c.fetchone()
    conn.close()
    return user_data

def get_all_users():
    """Retorna todos os usuários do banco de dados."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT name, email, role FROM userstable')
    all_users = c.fetchall()
    conn.close()
    return all_users
    
def update_user(email, new_name, new_role):
    """Atualiza o nome e o perfil de um usuário."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('UPDATE userstable SET name = ?, role = ? WHERE email = ?', (new_name, new_role, email))
    conn.commit()
    conn.close()

def update_user_password(email, new_password):
    """Atualiza a senha de um usuário."""
    conn = get_db_connection()
    c = conn.cursor()
    hashed_password = make_hashes(new_password)
    c.execute('UPDATE userstable SET password = ? WHERE email = ?', (hashed_password, email))
    conn.commit()
    conn.close()

def delete_user(email):
    """Deleta um usuário do banco de dados."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('DELETE FROM userstable WHERE email = ?', (email,))
    conn.commit()
    conn.close()

def setup_initial_admin():
    """Verifica se o admin inicial existe e, se não, o cria."""
    if not get_user("admin@email.com"):
        add_user("Administrador", "admin@email.com", "12345", "admin")
