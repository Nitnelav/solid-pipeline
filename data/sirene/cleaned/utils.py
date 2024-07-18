def get_st20(st8, employees):
    if st8 == 1:
        return 1

    if st8 == 2:
        if employees <= 2:
            return 2
        elif employees <= 6:
            return 3
        else:
            return 4

    if st8 == 3:
        if employees <= 4:
            return 5
        elif employees <= 10:
            return 6
        else:
            return 7

    if st8 == 4:
        if employees <= 6:
            return 8
        elif employees <= 19:
            return 9
        else:
            return 10

    if st8 == 5:
        if employees <= 70:
            return 11
        else:
            return 12

    if st8 == 6:
        if employees <= 2:
            return 13
        elif employees <= 5:
            return 14
        else:
            return 15

    if st8 == 7:
        if employees <= 4:
            return 16
        elif employees <= 18:
            return 17
        else:
            return 18

    if st8 == 8:
        if employees <= 14:
            return 19
        else:
            return 20

    return 0
