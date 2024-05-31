import subprocess

scripts_paths = ['./control_1.py', './control_2.py', './control_3.py', './control_4.py', './update.py']

procesos = [subprocess.Popen(f'python {script}', shell=True) for script in scripts_paths]

for proceso in procesos:
    proceso.wait()