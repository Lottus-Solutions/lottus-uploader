import csv
import random
import string
import sys

def gerar_valor_exemplo(texto_base):
    """Gera um valor aleatório simples a partir de um exemplo"""
    if texto_base.replace('.', '', 1).isdigit():
        # se for número, gera um float similar
        return round(random.uniform(1, 9999), 2)
    elif "@" in texto_base:
        # se parecer email
        usuario = ''.join(random.choices(string.ascii_lowercase, k=6))
        dominio = random.choice(["gmail.com", "hotmail.com", "empresa.com"])
        return f"{usuario}@{dominio}"
    else:
        # texto genérico
        base = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, 12)))
        return base

def gerar_csv(modelo, destino, qtd_linhas):
    with open(modelo, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        linhas = list(reader)

    cabecalho = linhas[0]
    exemplo = linhas[1] if len(linhas) > 1 else ["exemplo"] * len(cabecalho)

    with open(destino, "w", newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(cabecalho)
        for _ in range(qtd_linhas):
            linha = [gerar_valor_exemplo(exemplo[i]) for i in range(len(cabecalho))]
            writer.writerow(linha)

    print(f"✅ Arquivo gerado com sucesso: {destino}")
    print(f"Total de linhas: {qtd_linhas}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python gerar_csv.py <arquivo_modelo.csv> <quantidade_linhas>")
        sys.exit(1)

    arquivo_modelo = sys.argv[1]
    qtd = int(sys.argv[2])
    gerar_csv(arquivo_modelo, "livros_teste_gerado.csv", qtd)
