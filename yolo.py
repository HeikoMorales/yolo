import cv2
from ultralytics import YOLO
import pandas as pd
from datetime import datetime
import os


def send_csv_to_hadoop(path, file_name):
    from hdfs import InsecureClient

    client = InsecureClient('http://nombre_del_servidor:puerto', user='nombre_de_usuario')

    fichero_hdfs = f"/hdfs/yolo/points_csv/{file_name}"

    client.upload(hdfs_path=fichero_hdfs, local_path=path)


labels = ["nose", "left_eye", "right_eye", "left_ear", "right_ear", "left_shoulder",
          "right_shoulder", "left_elbow", "right_elbow", "left_wrist", "right_wrist",
          "left_hip", "right_hip", "left_knee", "right_knee", "left_ankle", "right_ankle"]

model = YOLO('yolov8m-pose.pt')

video_path = 0
cap = cv2.VideoCapture(video_path)
cap.set(cv2.CAP_PROP_FPS, 15)

data = []
x = 0
session = str(datetime.now().strftime("%Y%m%d%H%M%S"))

session_folder = f"C:/Users/aritz/OneDrive/Escritorio/MUUUH/Master/Semestre 2/DI/yolo/runs/pose/images/user1_{session}"
os.makedirs(session_folder, exist_ok=True)

while cap.isOpened():

    success, frame = cap.read()
    if success:
        results = model(frame, save=False)

        annotated_frame = results[0].plot()
        keypoints = results[0].keypoints.xy.cpu().numpy()[0]

        df = pd.DataFrame(keypoints, columns=['X', 'Y'])
        df['Label'] = labels
        df['Usuario'] = 'user1'
        df['FrameID'] = x
        df['Session'] = session
        data.append(df)

        cv2.imshow('YOLOv8', annotated_frame)
        cv2.imwrite(f"{session_folder}/user1_{session}_{str(x)}.jpg", annotated_frame)
        x += 1

        if cv2.waitKey(5) & 0xFF == 27:
            final_df = pd.concat(data, ignore_index=True)
            final_df.to_csv(f"C:/Users/aritz/OneDrive/Escritorio/MUUUH/Master/Semestre 2/DI/yolo/runs/pose/data/user1_{session}.csv", index=False)
            # send_csv_to_hadoop(f"C:/Users/aritz/OneDrive/Escritorio/MUUUH/Master/Semestre 2/DI/yolo/runs/pose/data/user1_{session}.csv", 'user1_{session}.csv')
            break
    else:
        break

cap.release()