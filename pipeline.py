import os
import subprocess

# List các Exercise bạn muốn chạy
exercises = ['Exercise-1', 'Exercise-2', 'Exercise-3', 'Exercise-4', 'Exercise-5']

# Hàm kiểm tra xem image đã có chưa
def check_image_exists(image_name):
    result = subprocess.run(['docker', 'images', '-q', image_name], stdout=subprocess.PIPE)
    return result.stdout.decode().strip() != ''

# Hàm build image nếu chưa có
def build_image(exercise_name):
    print(f"Image {exercise_name} chưa có, bắt đầu build...")
    result = subprocess.run(['docker', 'build', '-t', exercise_name, f'D:/NMKTDL/data-engineering-practice-Le_Trung_Huu/Exercises/{exercise_name}'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        print(f"Build image {exercise_name} thất bại. Dừng pipeline.")
        print(result.stderr.decode())
        exit(1)
    print(f"Build image {exercise_name} thành công.")

# Hàm chạy docker-compose
def run_docker_compose(exercise_name):
    print(f"Đang chạy {exercise_name} bằng image {exercise_name}...")
    compose_file = f'D:/NMKTDL/data-engineering-practice-Le_Trung_Huu/Exercises/{exercise_name}/docker-compose.yml'
    result = subprocess.run(['docker-compose', '-f', compose_file, 'up'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        print(f"Lỗi khi chạy {exercise_name}. Dừng pipeline.")
        print(result.stderr.decode())
        exit(1)
    
    # Kiểm tra logs của các container
    print("Kiểm tra logs của các container...")
    logs_result = subprocess.run(['docker-compose', '-f', compose_file, 'logs'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(logs_result.stdout.decode())  # In logs để xem chi tiết

    print(f"{exercise_name} đã hoàn tất!")

# Pipeline
def run_pipeline():
    for exercise in exercises:
        # Kiểm tra image đã tồn tại chưa
        if not check_image_exists(exercise.lower()):
            build_image(exercise)
        else:
            print(f"Image {exercise} đã có sẵn.")
        
        # Chạy docker-compose cho bài
        run_docker_compose(exercise)

    print("\nPipeline hoàn tất!")

if __name__ == "__main__":
    run_pipeline()
