import tkinter as tk
from tkinter import scrolledtext
from tkinter import messagebox


class App(tk.Tk):
    def __init__(self):
        super().__init__()

        # ======================================== Root window ========================================
        self.title("QEP Difference Explainer")
        screen_width, screen_height = self.winfo_screenwidth(), self.winfo_screenheight()  # Get screen width and height
        window_width, window_height = int(0.9*screen_width), int(0.9*screen_height)  # Window width and height will be 90% of screen
        # Coordinates of upper left corner of window to display in the center
        x_coord = int((screen_width/2) - (window_width/2))
        y_coord = int((screen_height/2) - (window_height/2))
        self.geometry(f"{window_width}x{window_height}+{x_coord}+{y_coord}")  # Place window
        
        # ======================================== Query input form ========================================
        # Query label row
        self.q_label_row = tk.Frame(self)
        self.q_label_row.pack()
        # Q1 label
        self.q1_label = tk.Label(self.q_label_row, text="Enter query Q1:")
        self.q1_label.pack(side="left")
        # Q2 label
        self.q2_label = tk.Label(self.q_label_row, text="Enter query Q2:")
        self.q2_label.pack(side="right")

        # Query textbox row
        self.q_textbox_row = tk.Frame(self)
        self.q_textbox_row.pack()
        # Q1 textbox
        self.q1_textbox = scrolledtext.ScrolledText(self.q_textbox_row, height=10)
        self.q1_textbox.pack(side="left")
        # Q2 textbox
        self.q2_textbox = scrolledtext.ScrolledText(self.q_textbox_row, height=10)
        self.q2_textbox.pack(side="right")

        # Query button row
        self.q_button_row = tk.Frame(self)
        self.q_button_row.pack()
        # Query submit button
        self.q_submit_button = tk.Button(self.q_button_row, text="Submit")
        self.q_submit_button.pack()

        # ======================================== Postgresql QEP output ========================================
        # QEP label row
        self.p_label_row = tk.Frame(self)
        self.p_label_row.pack()
        # P1 label
        self.p1_label = tk.Label(self.p_label_row, text="QEP P1:")
        self.p1_label.pack(side="left")
        # P2 label
        self.p2_label = tk.Label(self.p_label_row, text="QEP P2:")
        self.p2_label.pack(side="right")

        # QEP textbox row
        self.p_textbox_row = tk.Frame(self)
        self.p_textbox_row.pack()
        # P1 textbox
        self.p1_textbox = scrolledtext.ScrolledText(self.p_textbox_row, height=15)
        self.p1_textbox.pack(side="left")
        self.p1_textbox.configure(state="disabled")  # Make text read-only
        # P2 textbox
        self.p2_textbox = scrolledtext.ScrolledText(self.p_textbox_row, height=15)
        self.p2_textbox.pack(side="right")
        self.p2_textbox.configure(state="disabled")  # Make text read-only

        # ======================================== Explanation output ========================================
        # Explanation label row
        self.exp_label_row = tk.Frame(self)
        self.exp_label_row.pack()
        # Exp1 label
        self.exp1_label = tk.Label(self.exp_label_row, text="Explanation 1:")
        self.exp1_label.pack(side="left")
        # Exp2 label
        self.exp2_label = tk.Label(self.exp_label_row, text="Explanation 2:")
        self.exp2_label.pack(side="right")

        # Explanation textbox row
        self.exp_textbox_row = tk.Frame(self)
        self.exp_textbox_row.pack()
        # Exp1 textbox
        self.exp1_textbox = scrolledtext.ScrolledText(self.exp_textbox_row, height=15)
        self.exp1_textbox.pack(side="left")
        self.exp1_textbox.configure(state="disabled")  # Make textbox read-only
        # Exp2 textbox
        self.exp2_textbox = scrolledtext.ScrolledText(self.exp_textbox_row, height=15)
        self.exp2_textbox.pack(side="right")
        self.exp2_textbox.configure(state="disabled")  # Make textbox read-only

    
    def display_error_message(self, text):
        messagebox.showerror(title="Error", message=text)

    
    def get_query_input(self):
        return self.q1_textbox.get("1.0", "end-1c")
    

    def display_query_plan(self, text):
        self.p1_textbox.configure(state="normal")    # Allow write in textbox
        self.p1_textbox.delete("1.0", "end")         # Clear old text
        self.p1_textbox.insert("end", text)          # Insert new text
        self.p1_textbox.configure(state="disabled")  # Make textbox read-only

    
    def display_explanation(self, text):
        self.exp1_textbox.configure(state="normal")    # Allow write in textbox
        self.exp1_textbox.delete("1.0", "end")         # Clear old text
        self.exp1_textbox.insert("end", text)          # Insert new text
        self.exp1_textbox.configure(state="disabled")  # Make textbox read-only
