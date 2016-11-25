if __name__=='__main__':
    from Tkinter import *
    canvas=Canvas(width=400,height=400,bg='white')
    canvas.pack()
 
    x0=0
    y0=220
    x1=400
    y1=400
    canvas.create_rectangle(x0,y0,x1,y1,fill='blue')
 
    canvas.create_arc(100,130,300,310,start=0,extent=180,fill='red')
 
    canvas.create_line(280,70,250,130,width=5,fill='yellow')  
    canvas.create_line(340,120,290,160,width=5,fill='yellow')
    canvas.create_line(370,200,310,200,width=5,fill='yellow')
    canvas.create_line(200,50,200,120,width=5,fill='yellow')
    canvas.create_line(120,70,150,130,width=5,fill='yellow')
    canvas.create_line(60,120,110,160,width=5,fill='yellow')
    canvas.create_line(30,200,90,200,width=5,fill='yellow')
    mainloop()
