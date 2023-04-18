package com.github.dataswitch.game;
import java.awt.*;
import java.awt.event.*;
import java.util.Random;
import javax.swing.*;

public class MineSweeper extends JFrame {
    private final int ROWS = 10;        // 行数
    private final int COLS = 10;        // 列数
    private final int NUM_MINES = 10;   // 地雷数目
    private boolean[][] mines = new boolean[ROWS][COLS];  // 地雷数组
    private int[][] grid = new int[ROWS][COLS];           // 数字方格数组
    private JButton[][] buttons = new JButton[ROWS][COLS];// 按钮数组
    
    public MineSweeper() {
        setTitle("Mine Sweeper");       // 标题
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(500, 500);              // 宽和高都是500像素
        setLocationRelativeTo(null);    // 将窗口设置为居中
        setResizable(false);            // 禁止用户调整大小
        
        getContentPane().setLayout(new GridLayout(ROWS, COLS));  // 将GridLayout设置为主窗口布局
        
        initGrid();         // 初始化数字方格数组
        placeMines();       // 放置地雷
        calculateAdjacentSquares(); // 计算数字方格数组
        createButtons();    // 创建按钮数组
        
        setVisible(true);   // 显示主窗口
    }
    
    private void initGrid() {
        for (int i = 0; i < ROWS; i++) {
            for (int j = 0; j < COLS; j++) {
                mines[i][j] = false;        // 初始化地雷数组
                grid[i][j] = 0;             // 初始化数字方格数组
            }
        }
    }
    
    private void placeMines() {
        int count = 0;
        Random rand = new Random();         
        while (count < NUM_MINES) {            
            int row = rand.nextInt(ROWS);
            int col = rand.nextInt(COLS);
            if (!mines[row][col]) {
                mines[row][col] = true;     // 在该格子放置地雷
                count++;                    // 游戏地雷数加一
            }
        }
    }
    
    private void calculateAdjacentSquares() {
        for (int i = 0; i < ROWS; i++) {
            for (int j = 0; j < COLS; j++) {
                if (!mines[i][j]) {
                    int count = 0;
                    if (i > 0 && mines[i-1][j]) count++;
                    if (i < ROWS-1 && mines[i+1][j]) count++;
                    if (j > 0 && mines[i][j-1]) count++;
                    if (j < COLS-1 && mines[i][j+1]) count++;
                    if (i > 0 && j > 0 && mines[i-1][j-1]) count++;
                    if (i > 0 && j < COLS-1 && mines[i-1][j+1]) count++;
                    if (i < ROWS-1 && j > 0 && mines[i+1][j-1]) count++;
                    if (i < ROWS-1 && j < COLS-1 && mines[i+1][j+1]) count++;
                    grid[i][j] = count;          // 设置数字方格值
                }
            }
        }
    }
    
    private void createButtons() {
        for (int i = 0; i < ROWS; i++) {
            for (int j = 0; j < COLS; j++) {
                JButton button = new JButton();
                button.setPreferredSize(new Dimension(40, 40));   // 按钮大小
                button.setFont(new Font("Arial", Font.BOLD, 20));  // 设置按钮文本字体
                
                // 将ButtonListener添加到每个按钮中 
                ButtonListener buttonListener = new ButtonListener(i, j);
				button.addActionListener(buttonListener); 
                button.addMouseListener(buttonListener);
                
                buttons[i][j] = button;     
                getContentPane().add(button);  // 将按钮添加到主窗口中
            }
        }
    }
    
    private void revealSquare(int row, int col) {
        JButton btn = buttons[row][col];
		btn.setEnabled(false);        // 将按钮设置为不可用
		
        if (mines[row][col]) {
            btn.setText("*");        // 按钮标记为地雷
            JOptionPane.showMessageDialog(null, "You lost!"); // 弹出失败窗口
            gameOver();
        } else {
            btn.setText(Integer.toString(grid[row][col])); // 显示数字方格值
            
            if (grid[row][col] == 0) {      // 如果点击到的数字方格值为0,那么需要递归处理周围的数字方格
                if (row > 0 && col > 0 && buttons[row-1][col-1].isEnabled()) revealSquare(row-1, col-1);
                if (row > 0 && buttons[row-1][col].isEnabled()) revealSquare(row-1, col);
                if (row > 0 && col < COLS-1 && buttons[row-1][col+1].isEnabled()) revealSquare(row-1, col+1);
                if (col > 0 && buttons[row][col-1].isEnabled()) revealSquare(row, col-1);
                if (col < COLS-1 && buttons[row][col+1].isEnabled()) revealSquare(row, col+1);
                if (row < ROWS-1 && col > 0 && buttons[row+1][col-1].isEnabled()) revealSquare(row+1, col-1);
                if (row < ROWS-1 && buttons[row+1][col].isEnabled()) revealSquare(row+1, col);
                if (row < ROWS-1 && col < COLS-1 && buttons[row+1][col+1].isEnabled()) revealSquare(row+1, col+1);
            }
            checkWin();             // 计算是否赢得游戏
        }
    }

	private void gameOver() {
		System.exit(0);        // 结束程序
	}
    
	public void setFlag(int row, int col) {
		JButton btn = buttons[row][col];
		String flagText = "F";
		if(flagText.equals(btn.getText())) {
			btn.setText("");
		}else {
			if(!btn.getText().isEmpty()) { //已经打开，并且是数字
				return;
			}
			
			btn.setText(flagText);
		}
	}
	
    private void checkWin() {
        boolean win = true;
        for (int i = 0; i < ROWS; i++) {
            for (int j = 0; j < COLS; j++) {
                if (!mines[i][j] && buttons[i][j].isEnabled()) {
                    win = false;    // 游戏未结束
                    break;
                }
            }
            if (!win) break;
        }
        if (win) {
            JOptionPane.showMessageDialog(null, "You win!"); // 获胜窗口
            gameOver();
        }
    }
    
    private class ButtonListener extends MouseAdapter implements ActionListener {
        private int row, col;
        
        public ButtonListener(int row, int col) {
            this.row = row;
            this.col = col;
        }
        
        public void actionPerformed(ActionEvent e) {
            revealSquare(row, col);     // 点击按钮后处理按钮点击事件
        }
        
        @Override
        public void mousePressed(MouseEvent e) {
        	if (SwingUtilities.isRightMouseButton(e)) {
                System.out.println("Right button clicked, row:"+row+" col:"+col);
                setFlag(row,col);
            }
        }
    }
    
    public static void main(String[] args) {
        new MineSweeper();      // 启动程序
    }


    
}