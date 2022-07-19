enum States {

     START{
        @Override
        boolean count(){
            return false;
        }

        @Override
        public States nextState(){
            return RUNNING;
        }

         @Override
         public boolean checkUpgrade(){
            return false;
         }
    },

    RUNNING{
        int numRows = 0;
        Notifications notifications = Notifications.NOUPGRADE;
        @Override
        boolean count(){
            numRows++;
            return true;
        }
        @Override
        public States nextState(){
            return END;
        }

        @Override
        public boolean checkUpgrade(){
            if(numRows>69){
                notifications = Notifications.UPGRADE;
            }
            return true;
        }
    },

    END{
        @Override
        boolean count(){
        return false;
        }
        @Override
        public States nextState(){
            return START;
        }
        @Override
        public boolean checkUpgrade(){
            return false;
        }
    };
    abstract boolean count();
    abstract States nextState();
    abstract boolean checkUpgrade();
}
