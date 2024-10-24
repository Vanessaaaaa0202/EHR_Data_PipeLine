import os
import shutil

# 定义源文件夹的路径（顶层目录）
source_folder = '/Users/sk_sunflower163.com/Desktop/fhir'
# 定义目标文件夹的路径（所有json文件将被移动到这里）
destination_folder = '/Users/sk_sunflower163.com/Desktop/fhir_all_jsons'

# 如果目标文件夹不存在，则创建它
if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)

# 遍历源文件夹下的所有文件和子文件夹
for root, dirs, files in os.walk(source_folder):
    for file in files:
        # 只处理json文件
        if file.endswith('.json'):
            # 构建文件的完整路径
            file_path = os.path.join(root, file)
            # 将文件移动到目标文件夹
            shutil.move(file_path, os.path.join(destination_folder, file))

print(f"所有json文件已移动到 {destination_folder} 文件夹")
