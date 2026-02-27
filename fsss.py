from pathlib import Path
import argparse
import random
import matplotlib.pyplot as plt
import sys
from dataclasses import dataclass, field


@dataclass
class Proceso:
    pid: int
    grupo: int
    tiempo_llegada: int
    tiempo_rafaga: int

    # --- Variables de estado y métricas ---
    tiempo_restante: int = field(init=False)
    tiempo_primera_ejecucion: int | None = field(default=None, init=False)
    tiempo_finalizacion: int = field(default=0, init=False)
    tiempo_retorno: int = field(default=0, init=False)
    tiempo_espera: int = field(default=0, init=False)
    tiempo_respuesta: int = field(default=0, init=False)
    completado: bool = field(default=False, init=False)

    def __post_init__(self):
        """Inicializa el tiempo restante."""
        self.tiempo_restante = self.tiempo_rafaga

    def ejecutar(self, tiempo_asignado: int, tiempo_actual_sistema: int) -> int:
        """
        Ejecuta el proceso por un máximo del tiempo asignado.
        Actualiza sus métricas internas y retorna cuánto tiempo consumió.
        """
        # Registrar métricas de la primera vez que es ejecutado
        if self.tiempo_primera_ejecucion is None:
            self.tiempo_primera_ejecucion = tiempo_actual_sistema
            self.tiempo_respuesta = self.tiempo_primera_ejecucion - self.tiempo_llegada

        tiempo_a_consumir = min(self.tiempo_restante, tiempo_asignado)

        self.tiempo_restante -= tiempo_a_consumir
        tiempo_fin_bloque = tiempo_actual_sistema + tiempo_a_consumir

        # Si el proceso termina en este turno, calculamos sus métricas finales
        if self.tiempo_restante <= 0:
            self.completado = True
            self.tiempo_finalizacion = tiempo_fin_bloque
            self.tiempo_retorno = self.tiempo_finalizacion - self.tiempo_llegada
            self.tiempo_espera = self.tiempo_retorno - self.tiempo_rafaga

        return tiempo_a_consumir


@dataclass
class BloqueGantt:
    proceso: int
    grupo: int
    inicio: int
    fin: int

    @property
    def duracion(self) -> float:
        """Calcula la duración del bloque."""
        return self.fin - self.inicio


class SimuladorFairShare:
    tiempo_actual: int
    procesos_pendientes: list[Proceso]
    terminados: list[Proceso]
    simulacion_terminada: bool
    quantum_global: int
    colas_grupos: dict[int, list[Proceso]]
    cola_turnos_grupo: list[int]
    diagrama_gantt: list[BloqueGantt]

    def __init__(self, quantum_global: int):
        self.quantum_global = quantum_global
        self._reiniciar_estado()

    def _reiniciar_estado(self):
        self.tiempo_actual = 0
        self.procesos_pendientes = []
        self.colas_grupos = {}
        self.cola_turnos_grupo = []
        self.terminados = []
        self.diagrama_gantt = []
        self.simulacion_terminada = False

    def _actualizar_llegadas(self):
        """Mueve los procesos que ya llegaron de 'pendientes' a las colas de sus grupos."""
        while (
            self.procesos_pendientes
            and self.procesos_pendientes[0].tiempo_llegada <= self.tiempo_actual
        ):
            p = self.procesos_pendientes.pop(0)

            # Si el grupo no existe en el diccionario, lo agregamos
            if p.grupo not in self.colas_grupos:
                self.colas_grupos[p.grupo] = []

            # Agregamos el proceso a la cola de su grupo
            self.colas_grupos[p.grupo].append(p)

            # Si el grupo no está en la cola de turnos
            # (porque es nuevo o porque se había quedado sin procesos antes), lo volvemos a agregar.
            if p.grupo not in self.cola_turnos_grupo:
                self.cola_turnos_grupo.append(p.grupo)

    def paso(self):
        """
        Ejecuta el turno completo de un grupo, dividiendo su quantum entre
        todos sus procesos activos (Round Robin interno).
        """
        if self.simulacion_terminada:
            return

        self._actualizar_llegadas()

        # Limpiamos grupos que se quedaron sin procesos
        grupos_activos = [g for g in self.colas_grupos if self.colas_grupos[g]]
        self.cola_turnos_grupo = [
            g for g in self.cola_turnos_grupo if g in grupos_activos
        ]

        if not self.cola_turnos_grupo:
            if self.procesos_pendientes:
                self.tiempo_actual = self.procesos_pendientes[0].tiempo_llegada
                self._actualizar_llegadas()
                grupos_activos = [g for g in self.colas_grupos if self.colas_grupos[g]]
                self.cola_turnos_grupo = [
                    g for g in self.cola_turnos_grupo if g in grupos_activos
                ]
            else:
                self.simulacion_terminada = True
                return

        # --- Lógica Fair Share con Round Robin Interno ---
        quantum_grupo = self.quantum_global // len(self.cola_turnos_grupo)
        grupo_actual = self.cola_turnos_grupo.pop(0)
        self.cola_turnos_grupo.append(grupo_actual)

        # Obtenemos la cantidad de procesos listos en el grupo ESTE momento
        procesos_en_cola = len(self.colas_grupos[grupo_actual])
        quantum_proceso = max(quantum_grupo // procesos_en_cola, 1)

        # Ejecutamos cada proceso que estaba en la cola al inicio del turno
        for _ in range(procesos_en_cola):
            p = self.colas_grupos[grupo_actual].pop(0)

            tiempo_inicial = self.tiempo_actual
            tiempo_consumido = p.ejecutar(quantum_proceso, self.tiempo_actual)

            self.tiempo_actual += tiempo_consumido

            self.diagrama_gantt.append(
                BloqueGantt(
                    proceso=p.pid,
                    grupo=p.grupo,
                    inicio=tiempo_inicial,
                    fin=self.tiempo_actual,
                )
            )

            self._actualizar_llegadas()

            if p.completado:
                self.terminados.append(p)
            else:
                self.colas_grupos[grupo_actual].append(p)

    def simular(self, procesos: list[Proceso]) -> None:
        self._reiniciar_estado()
        self.procesos_pendientes = sorted(procesos, key=lambda p: p.tiempo_llegada)

        while not self.simulacion_terminada:
            self.paso()

    def mostrar_promedios(self):
        """Calcula e imprime los promedios de espera, retorno y respuesta."""
        if not self.terminados:
            print("No hay procesos terminados para calcular promedios.")
            return

        # Cálculos Globales
        n_total = len(self.terminados)
        suma_espera = sum(p.tiempo_espera for p in self.terminados)
        suma_retorno = sum(p.tiempo_retorno for p in self.terminados)
        suma_respuesta = sum(p.tiempo_respuesta for p in self.terminados)

        print("\n" + "=" * 55)
        print("      MÉTRICAS PROMEDIO DEL SISTEMA")
        print("=" * 55)
        print(f"Tiempo de Respuesta Promedio Global: {suma_respuesta / n_total:.2f}")
        print(f"Tiempo de Espera Promedio Global:    {suma_espera / n_total:.2f}")
        print(f"Tiempo de Retorno Promedio Global:   {suma_retorno / n_total:.2f}")
        print("-" * 55)

        # Cálculos por Grupo
        metricas_grupo = {}
        for p in self.terminados:
            if p.grupo not in metricas_grupo:
                metricas_grupo[p.grupo] = {
                    "espera": 0,
                    "retorno": 0,
                    "respuesta": 0,
                    "cantidad": 0,
                }

            metricas_grupo[p.grupo]["espera"] += p.tiempo_espera
            metricas_grupo[p.grupo]["retorno"] += p.tiempo_retorno
            metricas_grupo[p.grupo]["respuesta"] += p.tiempo_respuesta
            metricas_grupo[p.grupo]["cantidad"] += 1

        print("      PROMEDIOS POR GRUPO")
        print("-" * 55)

        # Imprime la tabla
        print(f"{'Grupo'} | {'Respuesta':>9} | {'Espera':>9} | {'Retorno':>9}")
        print("-" * 55)
        for grupo, datos in sorted(metricas_grupo.items()):
            prom_resp = datos["respuesta"] / datos["cantidad"]
            prom_espera = datos["espera"] / datos["cantidad"]
            prom_retorno = datos["retorno"] / datos["cantidad"]
            print(
                f"{grupo} | {prom_resp:>9.2f} | {prom_espera:>9.2f} | {prom_retorno:>9.2f}"
            )
        print("=" * 55 + "\n")

    def genera_gantt(self, salida: Path | None, sort: bool):
        """Genera una visualización del diagrama de Gantt usando Matplotlib."""
        if not self.diagrama_gantt:
            print("No hay datos. Ejecuta la simulación primero.")
            return

        height = len(self.terminados) / 5
        width = self.tiempo_actual / 15

        fig, ax = plt.subplots(figsize=(width, height))

        posiciones_y: dict[int, int] = {}
        indice_y = 10
        mapa_colores: dict[int, str] = {}
        paleta_colores = [
            "tab:blue",
            "tab:orange",
            "tab:green",
            "tab:red",
            "tab:purple",
            "tab:brown",
            "tab:pink",
        ]
        color_idx = 0

        if sort:
            bloques = sorted(self.diagrama_gantt, key=lambda b: b.proceso)
        else:
            bloques = self.diagrama_gantt

        for bloque in bloques:
            if bloque.proceso not in posiciones_y:
                posiciones_y[bloque.proceso] = indice_y
                indice_y += 10

            if bloque.grupo not in mapa_colores:
                mapa_colores[bloque.grupo] = paleta_colores[
                    color_idx % len(paleta_colores)
                ]
                color_idx += 1

        # Dibujar las sombras (Turnaround)
        for proceso in self.terminados:
            color = mapa_colores[proceso.grupo]

            ax.broken_barh(
                [(proceso.tiempo_llegada, proceso.tiempo_retorno)],
                (posiciones_y[proceso.pid] - 4, 8),
                facecolors=color,
                alpha=0.3,
            )

        # Dibujar los bloques sólidos con ancho dado por bloque.duracion
        for bloque in self.diagrama_gantt:
            color = mapa_colores[bloque.grupo]

            ax.broken_barh(
                [(bloque.inicio, bloque.duracion)],
                (posiciones_y[bloque.proceso] - 4, 8),
                facecolors=color,
                edgecolor="black",
            )

            ax.text(
                bloque.inicio + bloque.duracion / 2,
                posiciones_y[bloque.proceso],
                bloque.proceso,
                ha="center",
                va="center",
                color="white",
                weight="bold",
                fontsize=9,
            )

        # Configuración de ejes
        ax.set_yticks(list(posiciones_y.values()))
        ax.set_yticklabels(list(posiciones_y.keys()))
        ax.invert_yaxis()

        ax.set_xlabel("Tiempo de CPU")
        ax.set_ylabel("Procesos")
        ax.set_title(
            "Diagrama de Gantt - Fair Share Scheduling\n(Las sombras indican el Tiempo de Retorno total)"
        )
        ax.grid(True, axis="x", linestyle="--", alpha=0.7)

        # Leyenda
        grupos_ordenados = sorted(mapa_colores.keys())
        handles = [
            plt.Rectangle((0, 0), 1, 1, color=mapa_colores[g]) for g in grupos_ordenados
        ]
        ax.legend(handles, grupos_ordenados, title="Grupos", loc="upper right")

        plt.tight_layout()
        if salida is None:
            plt.show()
        else:
            plt.savefig(salida, dpi=300, bbox_inches="tight")


def cargar_procesos(ruta: Path, cantidad_grupos: int):
    lista_procesos: list[Proceso] = []
    rng = random.Random()
    try:
        with open(ruta, "r") as archivo:
            for linea in archivo:
                linea = linea.strip()
                if not linea:
                    continue

                partes = linea.split()
                if len(partes) >= 3:
                    pid = int(partes[0])
                    tiempo_llegada = int(partes[1])
                    tiempo_rafaga = int(partes[2])

                    grupo = rng.randint(1, cantidad_grupos)
                    lista_procesos.append(
                        Proceso(pid, grupo, tiempo_llegada, tiempo_rafaga)
                    )
    except FileNotFoundError:
        print(f"Error: Archivo '{ruta}' no encontrado.")
    return lista_procesos


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simulador de planificador Fair Share Scheduling"
    )

    _ = parser.add_argument(
        "-p",
        "--procesos",
        required=True,
        type=Path,
        help="Ruta al archivo de texto con los procesos",
    )
    _ = parser.add_argument(
        "-g", "--grupos", required=True, type=int, help="Cantidad de grupos a simular"
    )
    _ = parser.add_argument(
        "-q", "--quantum", required=True, type=int, help="Quantum global del sistema"
    )

    _ = parser.add_argument(
        "-s",
        "--sort",
        action="store_true",
        help="Ordena el diagrama de Gantt por ID del proceso.",
    )

    _ = parser.add_argument(
        "-o",
        "--output",
        required=False,
        type=Path,
        help="Ruta de salida para el diagrama de Gantt",
    )

    args = parser.parse_args()

    procesos = cargar_procesos(Path(args.procesos), args.grupos)

    if procesos:
        simulador = SimuladorFairShare(quantum_global=args.quantum)
        resultados = simulador.simular(procesos)

        simulador.mostrar_promedios()
        print("Generando gráfico Gantt...")
        simulador.genera_gantt(args.output, args.sort)
    else:
        print("No se encontraron procesos válidos para simular.", file=sys.stderr)
