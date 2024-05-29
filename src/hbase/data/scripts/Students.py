import json
from faker import Faker
import random
from datetime import datetime

fake = Faker()

def generate_student_data():
    student = {}

    # Personal info
    student['Personal'] = {
        'Nombres': fake.first_name(),
        'Apellidos': fake.last_name(),
        'Dirección': fake.address(),
        'CUI': fake.random_number(digits=9),
        'Nacionalidad': fake.country(),
        'Edad': fake.random_int(min=18, max=25)
    }

    # Académico info
    student['Académico'] = {
        'Carnet': fake.random_number(digits=8),
        'Grado': random.choice(['Primer año', 'Segundo año', 'Tercer año', 'Cuarto año', 'Quinto año']),
        'Cursos Aprobados': fake.random_int(min=0, max=10),
        'Cursos Pendientes': fake.random_int(min=0, max=5),
        'Cursos En Curso': fake.random_int(min=0, max=5),
        'Notas': fake.random_int(min=60, max=100),
        'Historial de faltas académicas': fake.random_int(min=0, max=5),
        'Carrera': fake.job(),
        'Facultad': fake.word()
    }

    # Cuenta info
    student['Cuenta'] = {
        'Mensualidad': '$' + str(fake.random_int(min=100, max=300)),
        'Pagos Pendientes': random.choice(['Sí', 'No']),
        'Tiene Beca': random.choice(['Sí', 'No']),
        'Cantidad Beca': '$' + str(fake.random_int(min=0, max=100)),
        'Tiene Crédito': random.choice(['Sí', 'No']),
        'Cantidad Crédito': '$' + str(fake.random_int(min=0, max=300))
    }

    return student

def generate_hbase_json():
    data = {}
    data['metadata'] = {
        'name': 'Estudiantes',
        'id': str(fake.uuid4()),
        'is_disabled': False,
        'created_at': str(datetime.now()),
        'n_rows': 1
    }
    data['data'] = {}

    student = generate_student_data()
    row_key = fake.word()  # La fila de ejemplo será una palabra aleatoria

    # Agregar datos a la fila
    data['data'][row_key] = {}
    for family, columns in student.items():
        data['data'][row_key][family] = {}
        for column, value in columns.items():
            data['data'][row_key][family][column] = {
                'n_versions': 1,
                'version1': {
                    'timestamp': str(datetime.now()),
                    'value': value
                }
            }

    return data

def save_json(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)

if __name__ == '__main__':
    student_json = generate_hbase_json()
    save_json(student_json, 'src/hbase/data/students.json')
    print("Archivo JSON generado exitosamente.")
