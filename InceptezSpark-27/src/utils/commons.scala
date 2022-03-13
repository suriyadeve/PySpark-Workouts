package utils

object commons {
  
  val getdiscount = (age:Int)=>
    {
      if(age < 10)
        5
      else if(age < 20)
        10
      else if(age < 40)
        15
      else if(age < 60)
        20
      else
        25
        
    }
   def getfullname(firstname:String,lastname:String):String=
   {
     return firstname +"-"+ lastname
        }
  
}

