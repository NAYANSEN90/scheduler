package com.example.scheduler;

public enum JobState {
    QUEUED("QUEUED", 0),
    RUNNING("RUNNING", 1),
    SUCCEEDED("SUCCEEDED", 2),
    FAILED("FAILED", 3);

    private String _state;
    private int _luaNum;

    JobState(String state, int num){
        this._state = state;
        this._luaNum = num;
    }

    @Override
    public String toString(){
        return _state;
    }

    public int getLUA(){
        return _luaNum;
    }
}
