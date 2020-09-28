import math


def ring_unit(x, z, y, done):
    if (x, y) not in done:
        done.add((x, y))
        agent.teleport((x, z, y))
        agent.place(1, 'down')


def ring(inner, outer, z):
    done = set()
    for x in range(outer):
        yinner = math.ceil(math.sqrt(max(0, inner * inner - x * x)))
        youter = math.ceil(math.sqrt(outer * outer - x * x))

        for i in range(yinner, youter):
            ring_unit(x, z, i, done)
            ring_unit(x, z, -i, done)
            ring_unit(-x, z, i, done)
            ring_unit(-x, z, -i, done)
            ring_unit(i, z, x, done)
            ring_unit(i, z, -x, done)
            ring_unit(-i, z, x, done)


def pie(radius, z):
    for x in range(radius):
        y = math.ceil(math.sqrt(radius * radius - x * x))
        agent.teleport((x,z,-y))
        for i in range(2*y+1):
            agent.place(1, 'down')
            agent.move('forward')
        agent.teleport((-x,z,-y))
        for i in range(2*y+1):
            agent.place(1, 'down')
            agent.move('forward')

def ring_destroy(inner, outer, z):
    for x in range(outer):
        yinner = math.ceil(math.sqrt(abs(x * x - inner * inner)))
        youter = math.ceil(math.sqrt(outer * outer - x * x))

        agent.teleport((x, z, yinner))
        for i in range(yinner, youter):
            agent.destroy('down')
            agent.move('forward')
        agent.teleport((x, z, -youter))
        for i in range(yinner, youter):
            agent.destroy('down')
            agent.move('forward')
        agent.teleport((-x, z, yinner))
        for i in range(yinner, youter):
            agent.destroy('down')
            agent.move('forward')
        agent.teleport((-x, z, -youter))
        for i in range(yinner, youter):
            agent.destroy('down')
            agent.move('forward')

        agent.teleport((yinner, z, x))
        for i in range(yinner, youter):
            agent.destroy('down')
            agent.move('left')
        agent.teleport((-youter, z, x))
        for i in range(yinner, youter):
            agent.destroy('down')
            agent.move('left')
        agent.teleport((yinner, z, -x))
        for i in range(yinner, youter):
            agent.destroy('down')
            agent.move('left')
        agent.teleport((-youter, z, -x))
        for i in range(yinner, youter):
            agent.destroy('down')
            agent.move('left')


def wall_destroy(radius, bottom, top):
    for x in range(radius):
        y = math.ceil(math.sqrt(radius * radius - x * x))
        agent.teleport((x, top, y - 1))
        for z in range(bottom, top):
            agent.destroy('down')
            agent.move('down')
        agent.teleport((y, top, x))
        for z in range(bottom, top):
            agent.destroy('down')
            agent.move('down')
        agent.teleport((x, top, -y))
        for z in range(bottom, top):
            agent.destroy('down')
            agent.move('down')
        agent.teleport((-y, top, x))
        for z in range(bottom, top):
            agent.destroy('down')
            agent.move('down')
        agent.teleport((-x, top, -y))
        for z in range(bottom, top):
            agent.destroy('down')
            agent.move('down')
        agent.teleport((-y, top, -x))
        for z in range(bottom, top):
            agent.destroy('down')
            agent.move('down')
        agent.teleport((-x, top, y))
        for z in range(bottom, top):
            agent.destroy('down')
            agent.move('down')
        agent.teleport((y, top, -x))
        for z in range(bottom, top):
            agent.destroy('down')
            agent.move('down')


def wall_unit(x, y, bottom, top, done):
    if (x, y) not in done:
        done.add((x, y))
        agent.teleport((x, bottom, y))
        for z in range(bottom, top):
            agent.place(1, 'down')
            agent.move('up')


def wall(radius, bottom, top):
    done = set()
    for x in range(radius):
        y = math.ceil(math.sqrt(radius * radius - x * x))
        wall_unit(x, y, bottom, top, done)
        wall_unit(x, -y, bottom, top, done)
        wall_unit(-x, y, bottom, top, done)
        wall_unit(-x, -y, bottom, top, done)
        wall_unit(y, x, bottom, top, done)
        wall_unit(y, -x, bottom, top, done)
        wall_unit(-y, -x, bottom, top, done)
        wall_unit(-y, x, bottom, top, done)
