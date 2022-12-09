from __future__ import annotations # PEP 563: postponed evaluation of annotations
import math

class Vector3d:
    def __init__(self, x : float, y : float, z : float):
        self.x = x
        self.y = y
        self.z = z

    def dot(self, v : Vector3d) -> float:
        return self.x * v.x + self.y * v.y + self.z * v.z

    def mod(self) -> float:
        return math.sqrt(self.x ** 2 + self.y ** 2 + self.z ** 2)

    def angle(self, v : Vector3d) -> float:
        return math.acos(self.dot(v) / (self.mod() * v.mod()))

    def __add__(self, v):
        return Vector3d(self.x + v.x, self.y + v.y, self.z + v.z)

    def __sub__(self, v):
        return Vector3d(self.x - v.x, self.y - v.y, self.z - v.z)

    def __mul__(self, s):
        return Vector3d(self.x * s, self.y * s, self.z * s)

    def __truediv__(self, s):
        return Vector3d(self.x / s, self.y / s, self.z / s)

    def __floordiv__(self, s):
        return Vector3d(self.x // s, self.y // s, self.z // s)

    def __gt__(self, v):
        # implement 'all' logic
        return all(self.x > v.x, self.y > v.y, self.z > v.z)

    def __lt__(self, v):
        # implement 'all' logic
        return all(self.x < v.x, self.y < v.y, self.z < v.z)

    def __ge__(self, v):
        # implement 'all' logic
        return all(self.x >= v.x, self.y >= v.y, self.z >= v.z)

    def __le__(self, v):
        # implement 'all' logic
        return all(self.x <= v.x, self.y <= v.y, self.z <= v.z)

    def __eq__(self, v):
        return all(self.x == v.x, self.y == v.y, self.z == v.z)

    def __ne__(self, v):
        return any(self.x != v.x, self.y != v.y, self.z != v.z)

    def __iadd__(self, v):
        self.x += v.x
        self.y += v.y
        self.z += v.z

    def __isub__(self, v):
        self.x -= v.x
        self.y -= v.y
        self.z -= v.z

    def __imul__(self, s):
        self.x *= s
        self.y *= s
        self.z *= s

    def __idiv__(self, s):
        self.x /= s
        self.y /= s
        self.z /= s

    def __ifloordiv__(self, s):
        self.x //= s
        self.y //= s
        self.z //= s    

    def __getitem__(self, i):
        if i == 0:
            return self.x
        elif i == 1:
            return self.y
        elif i == 2:
            return self.z
        else:
            raise Exception("Vector3d index out of bounds.")
