# main.py
import sys
import funciones

def ejecutar_funcion(nombre_funcion, var1, var2=None):
    # Buscar la función dinámicamente por nombre
    funcion = getattr(funciones, nombre_funcion, None)
    if not funcion:
        raise ValueError(f"La función '{nombre_funcion}' no existe.")

    # Convertir a número si es posible
    def try_cast(x):
        try:
            return float(x)
        except ValueError:
            return x

    var1 = try_cast(var1)
    var2 = try_cast(var2) if var2 is not None else None

    # Llamar a la función con uno o dos parámetros
    if var2 is not None:
        return funcion(var1, var2)
    else:
        return funcion(var1)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python main.py <nombre_funcion> <var1> [<var2>]")
        sys.exit(1)

    nombre_funcion = sys.argv[1]
    var1 = sys.argv[2]
    var2 = sys.argv[3] if len(sys.argv) > 3 else None

    resultado = ejecutar_funcion(nombre_funcion, var1, var2)
    print("Resultado:", resultado)
    # python main.py suma 5 3
    # python main.py resta 100.5 50
    # python main.py mayuscula_a_minuscula CODIGO_ERP_123
    # python main.py unir_campos ACTIVO 1234
